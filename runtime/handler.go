package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

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
