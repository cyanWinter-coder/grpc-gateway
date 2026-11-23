package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ForwardResponseStream forwards the stream from gRPC server to REST client.
func ForwardResponseStream(ctx context.Context, mux *ServeMux, marshaler Marshaler, w http.ResponseWriter, req *http.Request, recv func() (proto.Message, error), opts ...func(context.Context, http.ResponseWriter, proto.Message) error) {
	rc := http.NewResponseController(w)
	md, ok := ServerMetadataFromContext(ctx)
	if !ok {
		grpclog.Error("Failed to extract ServerMetadata from context")
		http.Error(w, "unexpected error", http.StatusInternalServerError)
		return
	}
	plan := planResponseMetadata(mux, md)
	plan.writeHeaders(w)

	if !mux.disableChunkedEncoding {
		w.Header().Set("Transfer-Encoding", "chunked")
	}
	// Trailers go through the same plan as the unary paths. The deferred flush
	// covers every exit below -- end of stream, upstream error, and a failed
	// write on client disconnect -- so closing the stream never leaves
	// announced metadata pending.
	forwardTrailers := !mux.disableChunkedEncoding && len(plan.trailer) > 0 && requestAcceptsTrailers(req)
	if forwardTrailers {
		plan.announceTrailers(w)
	}
	defer func() {
		if forwardTrailers {
			plan.writeTrailers(w)
		}
	}()

	if err := handleForwardResponseOptions(ctx, w, nil, opts); err != nil {
		// The error handler forwards the metadata itself; disarm the deferred
		// flush so the trailers are not written twice.
		forwardTrailers = false
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}

	var delimiter []byte
	if d, ok := marshaler.(Delimited); ok {
		delimiter = d.Delimiter()
	} else {
		delimiter = []byte("\n")
	}

	var wroteHeader bool
	for {
		resp, err := recv()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, delimiter)
			return
		}
		if err := handleForwardResponseOptions(ctx, w, resp, opts); err != nil {
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, delimiter)
			return
		}

		respRw, err := mux.forwardResponseRewriter(ctx, resp)
		if err != nil {
			grpclog.Errorf("Rewrite error: %v", err)
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, delimiter)
			return
		}

		if !wroteHeader {
			var contentType string
			if sct, ok := marshaler.(StreamContentType); ok {
				contentType = sct.StreamContentType(respRw)
			} else {
				contentType = marshaler.ContentType(respRw)
			}
			w.Header().Set("Content-Type", contentType)
		}

		var buf []byte
		httpBody, isHTTPBody := respRw.(*httpbody.HttpBody)
		switch {
		case respRw == nil:
			buf, err = marshaler.Marshal(errorChunk(status.New(codes.Internal, "empty response")))
		case isHTTPBody:
			buf = httpBody.GetData()
		default:
			result := map[string]interface{}{"result": respRw}
			if rb, ok := respRw.(responseBody); ok {
				result["result"] = rb.XXX_ResponseBody()
			}

			buf, err = marshaler.Marshal(result)
		}

		if err != nil {
			grpclog.Errorf("Failed to marshal response chunk: %v", err)
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, delimiter)
			return
		}
		if _, err := w.Write(buf); err != nil {
			grpclog.Errorf("Failed to send response chunk: %v", err)
			return
		}
		wroteHeader = true
		if _, err := w.Write(delimiter); err != nil {
			grpclog.Errorf("Failed to send delimiter chunk: %v", err)
			return
		}
		err = rc.Flush()
		if err != nil {
			if errors.Is(err, http.ErrNotSupported) {
				grpclog.Errorf("Flush not supported in %T", w)
				http.Error(w, "unexpected type of web server", http.StatusInternalServerError)
				return
			}
			grpclog.Errorf("Failed to flush response to client: %v", err)
			return
		}
	}
}

// responseMetadataPlan is the result of matching a response's ServerMetadata
// against the mux's outgoing matchers. It is computed once per response, before
// anything is written, so the success path (ForwardResponseMessage), the error
// path (DefaultHTTPErrorHandler) and the stream path (ForwardResponseStream)
// all merge metadata the same way, and the result never depends on write
// timing.
//
// The propagation rules are:
//
//   - Metadata keys are normalized to lower case before matching, so a matcher
//     sees the same exact key no matter how the MD was constructed. The HTTP
//     name a matcher returns is canonicalized once, here, so the name announced
//     in the Trailer header and the name the value is later written under
//     cannot drift apart.
//   - HeaderMD values only ever become response headers and TrailerMD values
//     only ever become response trailers: a value stays in the location its
//     matcher declared for it, and once the body has started nothing is
//     promoted into a header.
//   - Values keep their metadata order, and each value is written as its own
//     header field. An empty value is forwarded as an empty HTTP field value,
//     which RFC 7230 section 3.2 allows; values are never joined into a
//     comma-separated list, so an empty value cannot produce a malformed
//     separator.
//   - If the two matchers map different metadata keys to the same HTTP name,
//     the header entry wins the name and the trailer entry is dropped. A name
//     announced in the Trailer header is a trailer on the wire, so keeping both
//     would demote the header values instead.
type responseMetadataPlan struct {
	header  http.Header
	trailer http.Header
}

// planResponseMetadata matches md against the mux's outgoing header and
// trailer matchers. See responseMetadataPlan for the propagation rules.
func planResponseMetadata(mux *ServeMux, md ServerMetadata) responseMetadataPlan {
	var plan responseMetadataPlan
	if len(md.HeaderMD) > 0 {
		plan.header = matchResponseMetadata(mux.outgoingHeaderMatcher, md.HeaderMD)
	}
	if len(md.TrailerMD) > 0 {
		plan.trailer = matchResponseMetadata(mux.outgoingTrailerMatcher, md.TrailerMD)
		for k := range plan.header {
			delete(plan.trailer, k)
		}
	}
	return plan
}

func matchResponseMetadata(matcher HeaderMatcherFunc, md metadata.MD) http.Header {
	matched := make(http.Header, len(md))
	for k, vs := range md {
		// metadata.MD keys are lower case by convention, but nothing stops a
		// caller from building the map by hand; normalize so the matcher sees
		// one exact spelling per key.
		h, ok := matcher(strings.ToLower(k))
		if !ok {
			continue
		}
		if h = textproto.CanonicalMIMEHeaderKey(h); h == "" {
			continue
		}
		matched[h] = append(matched[h], vs...)
	}
	return matched
}

// writeHeaders writes the planned header metadata. It must run before the
// response body starts.
func (p responseMetadataPlan) writeHeaders(w http.ResponseWriter) {
	for k, vs := range p.header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
}

// announceTrailers declares the planned trailer names in the Trailer header,
// so the values written after the body are sent as trailers. It must run
// before the response body starts.
func (p responseMetadataPlan) announceTrailers(w http.ResponseWriter) {
	for k := range p.trailer {
		w.Header().Add("Trailer", k)
	}
}

// writeTrailers writes the planned trailer values under the names
// announceTrailers declared. It runs after the body; net/http emits values for
// announced names as trailers, so nothing here is promoted into a header.
func (p responseMetadataPlan) writeTrailers(w http.ResponseWriter) {
	for k, vs := range p.trailer {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
}

// responseBody interface contains method for getting field for marshaling to the response body
// this method is generated for response struct from the value of `response_body` in the `google.api.HttpRule`
type responseBody interface {
	XXX_ResponseBody() interface{}
}

// ForwardResponseMessage forwards the message "resp" from gRPC server to REST client.
func ForwardResponseMessage(ctx context.Context, mux *ServeMux, marshaler Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) {
	md, ok := ServerMetadataFromContext(ctx)
	var plan responseMetadataPlan
	if ok {
		plan = planResponseMetadata(mux, md)
		plan.writeHeaders(w)
	}

	// RFC 7230 https://tools.ietf.org/html/rfc7230#section-4.1.2
	// Unless the request includes a TE header field indicating "trailers"
	// is acceptable, as described in Section 4.3, a server SHOULD NOT
	// generate trailer fields that it believes are necessary for the user
	// agent to receive.
	doForwardTrailers := requestAcceptsTrailers(req)

	if ok && doForwardTrailers {
		plan.announceTrailers(w)
		w.Header().Set("Transfer-Encoding", "chunked")
	}

	contentType := marshaler.ContentType(resp)
	w.Header().Set("Content-Type", contentType)

	if err := handleForwardResponseOptions(ctx, w, resp, opts); err != nil {
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}
	respRw, err := mux.forwardResponseRewriter(ctx, resp)
	if err != nil {
		grpclog.Errorf("Rewrite error: %v", err)
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}
	var buf []byte
	if rb, ok := respRw.(responseBody); ok {
		buf, err = marshaler.Marshal(rb.XXX_ResponseBody())
	} else {
		buf, err = marshaler.Marshal(respRw)
	}
	if err != nil {
		grpclog.Errorf("Marshal error: %v", err)
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}

	if !doForwardTrailers && mux.writeContentLength {
		w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	}

	if _, err = w.Write(buf); err != nil && !errors.Is(err, http.ErrBodyNotAllowed) {
		grpclog.Errorf("Failed to write response: %v", err)
	}

	if ok && doForwardTrailers {
		plan.writeTrailers(w)
	}
}

func requestAcceptsTrailers(req *http.Request) bool {
	// "Te", not "TE": Header.Get canonicalises the key it is given, and a key that is already
	// canonical is returned as is, while any other spelling costs an allocated copy on every
	// call. This one runs once per response.
	te := req.Header.Get("Te")
	return strings.Contains(strings.ToLower(te), "trailers")
}

func handleForwardResponseOptions(ctx context.Context, w http.ResponseWriter, resp proto.Message, opts []func(context.Context, http.ResponseWriter, proto.Message) error) error {
	if len(opts) == 0 {
		return nil
	}
	for _, opt := range opts {
		if err := opt(ctx, w, resp); err != nil {
			return fmt.Errorf("error handling ForwardResponseOptions: %w", err)
		}
	}
	return nil
}

func handleForwardResponseStreamError(ctx context.Context, wroteHeader bool, marshaler Marshaler, w http.ResponseWriter, req *http.Request, mux *ServeMux, err error, delimiter []byte) {
	st := mux.streamErrorHandler(ctx, err)
	msg := errorChunk(st)
	if !wroteHeader {
		w.Header().Set("Content-Type", marshaler.ContentType(msg))
		w.WriteHeader(HTTPStatusFromCode(st.Code()))
	}
	buf, err := marshaler.Marshal(msg)
	if err != nil {
		grpclog.Errorf("Failed to marshal an error: %v", err)
		return
	}
	if _, err := w.Write(buf); err != nil {
		grpclog.Errorf("Failed to notify error to client: %v", err)
		return
	}
	if _, err := w.Write(delimiter); err != nil {
		grpclog.Errorf("Failed to send delimiter chunk: %v", err)
		return
	}
}

func errorChunk(st *status.Status) map[string]proto.Message {
	return map[string]proto.Message{"error": st.Proto()}
}
