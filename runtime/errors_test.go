package runtime_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestDefaultHTTPError(t *testing.T) {
	ctx := context.Background()

	statusWithDetails, _ := status.New(codes.FailedPrecondition, "failed precondition").WithDetails(
		&errdetails.PreconditionFailure{},
	)

	for i, spec := range []struct {
		err                  error
		status               int
		msg                  string
		marshaler            runtime.Marshaler
		contentType          string
		details              string
		fordwardRespRewriter runtime.ForwardResponseRewriter
		extractMessage       func(*testing.T)
	}{
		{
			err:         errors.New("example error"),
			status:      http.StatusInternalServerError,
			marshaler:   &runtime.JSONPb{},
			contentType: "application/json",
			msg:         "example error",
		},
		{
			err:         status.Error(codes.NotFound, "no such resource"),
			status:      http.StatusNotFound,
			marshaler:   &runtime.JSONPb{},
			contentType: "application/json",
			msg:         "no such resource",
		},
		{
			err:         statusWithDetails.Err(),
			status:      http.StatusBadRequest,
			marshaler:   &runtime.JSONPb{},
			contentType: "application/json",
			msg:         "failed precondition",
			details:     "type.googleapis.com/google.rpc.PreconditionFailure",
		},
		{
			err:         errors.New("example error"),
			status:      http.StatusInternalServerError,
			marshaler:   &CustomMarshaler{&runtime.JSONPb{}},
			contentType: "Custom-Content-Type",
			msg:         "example error",
		},
		{
			err: &runtime.HTTPStatusError{
				HTTPStatus: http.StatusMethodNotAllowed,
				Err:        status.Error(codes.Unimplemented, http.StatusText(http.StatusMethodNotAllowed)),
			},
			status:      http.StatusMethodNotAllowed,
			marshaler:   &runtime.JSONPb{},
			contentType: "application/json",
			msg:         "Method Not Allowed",
		},
		{
			err:         status.Error(codes.InvalidArgument, "example error"),
			status:      http.StatusBadRequest,
			marshaler:   &runtime.JSONPb{},
			contentType: "application/json",
			msg:         "bad request: example error",
			fordwardRespRewriter: func(ctx context.Context, response proto.Message) (any, error) {
				if s, ok := response.(*statuspb.Status); ok && strings.HasPrefix(s.Message, "example") {
					return &statuspb.Status{
						Code:    s.Code,
						Message: "bad request: " + s.Message,
						Details: s.Details,
					}, nil
				}
				return response, nil
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequestWithContext(ctx, "", "", nil) // Pass in an empty request to match the signature

			opts := []runtime.ServeMuxOption{}
			if spec.fordwardRespRewriter != nil {
				opts = append(opts, runtime.WithForwardResponseRewriter(spec.fordwardRespRewriter))
			}
			mux := runtime.NewServeMux(opts...)

			runtime.HTTPError(ctx, mux, spec.marshaler, w, req, spec.err)

			if got, want := w.Header().Get("Content-Type"), spec.contentType; got != want {
				t.Errorf(`w.Header().Get("Content-Type") = %q; want %q; on spec.err=%v`, got, want, spec.err)
			}
			if got, want := w.Code, spec.status; got != want {
				t.Errorf("w.Code = %d; want %d", got, want)
			}

			var st statuspb.Status
			if err := spec.marshaler.Unmarshal(w.Body.Bytes(), &st); err != nil {
				t.Errorf("marshaler.Unmarshal(%q, &body) failed with %v; want success", w.Body.Bytes(), err)
				return
			}

			if got, want := st.Message, spec.msg; !strings.Contains(got, want) {
				t.Errorf(`st.Message = %q; want %q; on spec.err=%v`, got, want, spec.err)
			}

			if spec.details != "" {
				if len(st.Details) != 1 {
					t.Errorf(`len(st.Details) = %v; want 1`, len(st.Details))
					return
				}
				if st.Details[0].TypeUrl != spec.details {
					t.Errorf(`details.type_url = %s; want %s`, st.Details[0].TypeUrl, spec.details)
				}
			}
		})
	}
}

func TestHTTPStreamError(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name             string
		err              error
		expectedStatus   *status.Status
		expectedResponse []byte
	}{
		{
			name:             "Simple error",
			err:              errors.New("simple error"),
			expectedStatus:   status.New(codes.Unknown, "simple error"),
			expectedResponse: []byte(`{"error":{"code":2,"message":"simple error"}}`),
		},
		{
			name:             "Invalid request error",
			err:              status.Error(codes.InvalidArgument, "invalid request"),
			expectedStatus:   status.New(codes.InvalidArgument, "invalid request"),
			expectedResponse: []byte(`{"error":{"code":3,"message":"invalid request"}}`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/", nil)

			mux := runtime.NewServeMux(runtime.WithStreamErrorHandler(
				runtime.DefaultStreamErrorHandler,
			))

			marshaler := &runtime.JSONPb{}

			runtime.HTTPStreamError(ctx, mux, marshaler, w, r, tc.err)

			if w.Code != http.StatusOK {
				t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
			}

			if !proto.Equal(status.Convert(tc.err).Proto(), tc.expectedStatus.Proto()) {
				t.Errorf("Expected status %v, got %v", tc.expectedStatus, status.Convert(tc.err))
			}

			if !bytes.Equal(w.Body.Bytes(), tc.expectedResponse) {
				t.Errorf("Expected response %s, got %s", tc.expectedResponse, w.Body.Bytes())
			}
		})
	}
}

// TestDefaultHTTPErrorTrailerParity verifies that the error path merges server
// metadata through the same rules as the success path: header metadata becomes
// response headers, trailer metadata is announced before the body and written
// after it, and multi-value order is preserved.
func TestDefaultHTTPErrorTrailerParity(t *testing.T) {
	md := runtime.ServerMetadata{
		HeaderMD:  metadata.Pairs("foo", "h1", "foo", "h2"),
		TrailerMD: metadata.Pairs("foo", "t1", "foo", "t2"),
	}
	ctx := runtime.NewServerMetadataContext(context.Background(), md)

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	req.Header.Set("Te", "trailers")
	resp := httptest.NewRecorder()

	mux := runtime.NewServeMux()
	runtime.DefaultHTTPErrorHandler(ctx, mux, &runtime.JSONPb{}, resp, req, status.Error(codes.NotFound, "missing"))

	w := resp.Result()
	_, _ = io.Copy(io.Discard, w.Body)
	defer w.Body.Close()

	if w.StatusCode != http.StatusNotFound {
		t.Fatalf("StatusCode %d want %d", w.StatusCode, http.StatusNotFound)
	}

	if got, want := w.Header["Grpc-Metadata-Foo"], []string{"h1", "h2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Header Grpc-Metadata-Foo %v want %v", got, want)
	}
	if got, want := w.Header.Values("Trailer"), []string{"Grpc-Trailer-Foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Trailer announcement %v want %v", got, want)
	}
	if got, want := w.Header.Get("Transfer-Encoding"), "chunked"; got != want {
		t.Errorf("Transfer-Encoding %q want %q", got, want)
	}
	if got, want := w.Trailer["Grpc-Trailer-Foo"], []string{"t1", "t2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Trailer Grpc-Trailer-Foo %v want %v", got, want)
	}
}

// TestDefaultHTTPErrorNoTrailerWithoutTE verifies the error path does not
// announce or write trailers when the client does not accept them.
func TestDefaultHTTPErrorNoTrailerWithoutTE(t *testing.T) {
	md := runtime.ServerMetadata{
		TrailerMD: metadata.Pairs("foo", "t1"),
	}
	ctx := runtime.NewServerMetadataContext(context.Background(), md)

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	resp := httptest.NewRecorder()

	mux := runtime.NewServeMux()
	runtime.DefaultHTTPErrorHandler(ctx, mux, &runtime.JSONPb{}, resp, req, status.Error(codes.NotFound, "missing"))

	w := resp.Result()
	_, _ = io.Copy(io.Discard, w.Body)
	defer w.Body.Close()

	if got := w.Header.Values("Trailer"); len(got) != 0 {
		t.Errorf("unexpected Trailer announcement %v", got)
	}
	if got := w.Header.Get("Transfer-Encoding"); got != "" {
		t.Errorf("unexpected Transfer-Encoding %q", got)
	}
	if len(w.Trailer) != 0 {
		t.Errorf("unexpected trailers %v", w.Trailer)
	}
}
