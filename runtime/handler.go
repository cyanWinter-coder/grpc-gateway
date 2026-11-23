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
	handleForwardResponseServerMetadata(w, mux, md)

	if !mux.disableChunkedEncoding {
		w.Header().Set("Transfer-Encoding", "chunked")
	}
	if err := handleForwardResponseOptions(ctx, w, nil, opts); err != nil {
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}

	var delimiter []byte
	if d, ok := marshaler.(Delimited); ok {
		delimiter = d.Delimiter()
	} else {
		delimiter = []byte("\n")
	}

	// All chunks and the error chunk use the registry snapshot the request
	// entered with, so a registration made mid-stream cannot change the
	// encoding halfway through the response.
	snapshot := marshalerSnapshotForRequest(mux, req)

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

		var buf []byte
		usedMarshaler := marshaler
		httpBody, isHTTPBody := respRw.(*httpbody.HttpBody)
		switch {
		case respRw == nil:
			buf, usedMarshaler, err = snapshot.marshalWithFallback(marshaler, errorChunk(status.New(codes.Internal, "empty response")))
		case isHTTPBody:
			buf = httpBody.GetData()
		default:
			result := map[string]interface{}{"result": respRw}
			if rb, ok := respRw.(responseBody); ok {
				result["result"] = rb.XXX_ResponseBody()
			}

			buf, usedMarshaler, err = snapshot.marshalWithFallback(marshaler, result)
		}

		if err != nil {
			grpclog.Errorf("Failed to marshal response chunk: %v", err)
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, delimiter)
			return
		}

		if !wroteHeader {
			w.Header().Set("Content-Type", streamContentType(usedMarshaler, respRw))
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

func handleForwardResponseServerMetadata(w http.ResponseWriter, mux *ServeMux, md ServerMetadata) {
	for k, vs := range md.HeaderMD {
		if h, ok := mux.outgoingHeaderMatcher(k); ok {
			for _, v := range vs {
				w.Header().Add(h, v)
			}
		}
	}
}

func handleForwardResponseTrailerHeader(w http.ResponseWriter, mux *ServeMux, md ServerMetadata) {
	for k := range md.TrailerMD {
		if h, ok := mux.outgoingTrailerMatcher(k); ok {
			w.Header().Add("Trailer", textproto.CanonicalMIMEHeaderKey(h))
		}
	}
}

func handleForwardResponseTrailer(w http.ResponseWriter, mux *ServeMux, md ServerMetadata) {
	for k, vs := range md.TrailerMD {
		if h, ok := mux.outgoingTrailerMatcher(k); ok {
			for _, v := range vs {
				w.Header().Add(h, v)
			}
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
	if ok {
		handleForwardResponseServerMetadata(w, mux, md)
	}

	// RFC 7230 https://tools.ietf.org/html/rfc7230#section-4.1.2
	// Unless the request includes a TE header field indicating "trailers"
	// is acceptable, as described in Section 4.3, a server SHOULD NOT
	// generate trailer fields that it believes are necessary for the user
	// agent to receive.
	doForwardTrailers := requestAcceptsTrailers(req)

	if ok && doForwardTrailers {
		handleForwardResponseTrailerHeader(w, mux, md)
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
	snapshot := marshalerSnapshotForRequest(mux, req)
	var buf []byte
	var usedMarshaler Marshaler
	if rb, ok := respRw.(responseBody); ok {
		buf, usedMarshaler, err = snapshot.marshalWithFallback(marshaler, rb.XXX_ResponseBody())
	} else {
		buf, usedMarshaler, err = snapshot.marshalWithFallback(marshaler, respRw)
	}
	if err != nil {
		grpclog.Errorf("Marshal error: %v", err)
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}
	if !sameMarshaler(usedMarshaler, marshaler) {
		// The preferred marshaler could not encode the message and a
		// compatible fallback from the same snapshot did; advertise that one.
		w.Header().Set("Content-Type", usedMarshaler.ContentType(respRw))
	}

	if !doForwardTrailers && mux.writeContentLength {
		w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	}

	if _, err = w.Write(buf); err != nil && !errors.Is(err, http.ErrBodyNotAllowed) {
		grpclog.Errorf("Failed to write response: %v", err)
	}

	if ok && doForwardTrailers {
		handleForwardResponseTrailer(w, mux, md)
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
	buf, usedMarshaler, merr := marshalerSnapshotForRequest(mux, req).marshalWithFallback(marshaler, msg)
	if !wroteHeader {
		contentType := marshaler.ContentType(msg)
		if !sameMarshaler(usedMarshaler, marshaler) {
			contentType = usedMarshaler.ContentType(msg)
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(HTTPStatusFromCode(st.Code()))
	}
	if merr != nil {
		grpclog.Errorf("Failed to marshal an error: %v", merr)
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

// streamContentType reports the Content-Type a marshaler uses for a chunk,
// preferring the stream-specific type when the marshaler implements it.
func streamContentType(marshaler Marshaler, v interface{}) string {
	if sct, ok := marshaler.(StreamContentType); ok {
		return sct.StreamContentType(v)
	}
	return marshaler.ContentType(v)
}

func errorChunk(st *status.Status) map[string]proto.Message {
	return map[string]proto.Message{"error": st.Proto()}
}
