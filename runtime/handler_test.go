package runtime_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/grpc-ecosystem/grpc-gateway/v2/runtime/internal/examplepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakeResponseBodyWrapper struct {
	proto.Message
}

// XXX_ResponseBody returns id of SimpleMessage
func (r fakeResponseBodyWrapper) XXX_ResponseBody() interface{} {
	resp := r.Message.(*pb.SimpleMessage)
	return resp.Id
}

func TestForwardResponseStream(t *testing.T) {
	type msg struct {
		pb  proto.Message
		err error
	}
	tests := []struct {
		name                   string
		msgs                   []msg
		statusCode             int
		responseBody           bool
		disableChunkedEncoding bool
	}{{
		name: "encoding",
		msgs: []msg{
			{&pb.SimpleMessage{Id: "One"}, nil},
			{&pb.SimpleMessage{Id: "Two"}, nil},
		},
		statusCode: http.StatusOK,
	}, {
		name:       "empty",
		statusCode: http.StatusOK,
	}, {
		name:       "error",
		msgs:       []msg{{nil, status.Errorf(codes.OutOfRange, "400")}},
		statusCode: http.StatusBadRequest,
	}, {
		name: "stream_error",
		msgs: []msg{
			{&pb.SimpleMessage{Id: "One"}, nil},
			{nil, status.Errorf(codes.OutOfRange, "400")},
		},
		statusCode: http.StatusOK,
	}, {
		name: "response body stream case",
		msgs: []msg{
			{fakeResponseBodyWrapper{&pb.SimpleMessage{Id: "One"}}, nil},
			{fakeResponseBodyWrapper{&pb.SimpleMessage{Id: "Two"}}, nil},
		},
		responseBody: true,
		statusCode:   http.StatusOK,
	}, {
		name: "response body stream error case",
		msgs: []msg{
			{fakeResponseBodyWrapper{&pb.SimpleMessage{Id: "One"}}, nil},
			{nil, status.Errorf(codes.OutOfRange, "400")},
		},
		responseBody: true,
		statusCode:   http.StatusOK,
	}, {
		name: "disable chunked encoding",
		msgs: []msg{
			{&pb.SimpleMessage{Id: "One"}, nil},
		},
		statusCode:             http.StatusOK,
		disableChunkedEncoding: true,
	}}

	newTestRecv := func(t *testing.T, msgs []msg) func() (proto.Message, error) {
		var count int
		return func() (proto.Message, error) {
			if count == len(msgs) {
				return nil, io.EOF
			} else if count > len(msgs) {
				t.Errorf("recv() called %d times for %d messages", count, len(msgs))
			}
			count++
			msg := msgs[count-1]
			return msg.pb, msg.err
		}
	}
	ctx := runtime.NewServerMetadataContext(context.Background(), runtime.ServerMetadata{})
	marshaler := &runtime.JSONPb{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recv := newTestRecv(t, tt.msgs)
			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			resp := httptest.NewRecorder()

			mux := runtime.NewServeMux()
			if tt.disableChunkedEncoding {
				mux = runtime.NewServeMux(runtime.WithDisableChunkedEncoding())
			}
			runtime.ForwardResponseStream(ctx, mux, marshaler, resp, req, recv)

			w := resp.Result()
			if w.StatusCode != tt.statusCode {
				t.Errorf("StatusCode %d want %d", w.StatusCode, tt.statusCode)
			}
			if !tt.disableChunkedEncoding {
				if h := w.Header.Get("Transfer-Encoding"); h != "chunked" {
					t.Errorf("ForwardResponseStream missing header chunked")
				}
			} else {
				if h := w.Header.Get("Transfer-Encoding"); h != "" {
					t.Errorf("ForwardResponseStream unexpected Transfer-Encoding header %s", h)
				}
			}
			body, err := io.ReadAll(w.Body)
			if err != nil {
				t.Errorf("Failed to read response body with %v", err)
			}
			w.Body.Close()
			if len(body) > 0 && w.Header.Get("Content-Type") != "application/json" {
				t.Errorf("Content-Type %s want application/json", w.Header.Get("Content-Type"))
			}

			var want []byte
			for i, msg := range tt.msgs {
				if msg.err != nil {
					if i == 0 {
						// Skip non-stream errors
						t.Skip("checking error encodings")
					}
					delimiter := marshaler.Delimiter()
					st := status.Convert(msg.err)
					b, err := marshaler.Marshal(map[string]proto.Message{
						"error": st.Proto(),
					})
					if err != nil {
						t.Errorf("marshaler.Marshal() failed %v", err)
					}
					errBytes := body[len(want):]
					if string(errBytes) != string(b)+string(delimiter) {
						t.Errorf("ForwardResponseStream() = \"%s\" want \"%s\"", errBytes, b)
					}

					return
				}

				var b []byte

				if tt.responseBody {
					// responseBody interface is in runtime package and test is in runtime_test package. hence can't use responseBody directly
					// So type casting to fakeResponseBodyWrapper struct to verify the data.
					rb, ok := msg.pb.(fakeResponseBodyWrapper)
					if !ok {
						t.Errorf("stream responseBody failed %v", err)
					}

					b, err = marshaler.Marshal(map[string]interface{}{"result": rb.XXX_ResponseBody()})
				} else {
					b, err = marshaler.Marshal(map[string]interface{}{"result": msg.pb})
				}

				if err != nil {
					t.Errorf("marshaler.Marshal() failed %v", err)
				}
				want = append(want, b...)
				want = append(want, marshaler.Delimiter()...)
			}

			if string(body) != string(want) {
				t.Errorf("ForwardResponseStream() = \"%s\" want \"%s\"", body, want)
			}
		})
	}
}

// A custom marshaler implementation, that doesn't implement the delimited interface
type CustomMarshaler struct {
	m *runtime.JSONPb
}

func (c *CustomMarshaler) Marshal(v interface{}) ([]byte, error)      { return c.m.Marshal(v) }
func (c *CustomMarshaler) Unmarshal(data []byte, v interface{}) error { return c.m.Unmarshal(data, v) }
func (c *CustomMarshaler) NewDecoder(r io.Reader) runtime.Decoder     { return c.m.NewDecoder(r) }
func (c *CustomMarshaler) NewEncoder(w io.Writer) runtime.Encoder     { return c.m.NewEncoder(w) }
func (c *CustomMarshaler) ContentType(v interface{}) string           { return "Custom-Content-Type" }

// marshalerStreamContentType implements Marshaler, but with the addition of a custom StreamContentType.
type marshalerStreamContentType struct {
	runtime.Marshaler
	CustomStreamContentType string
}

func (m marshalerStreamContentType) StreamContentType(interface{}) string {
	return m.CustomStreamContentType
}

func TestForwardResponseStreamCustomMarshaler(t *testing.T) {
	type msg struct {
		pb  proto.Message
		err error
	}
	marshaler := &CustomMarshaler{&runtime.JSONPb{}}

	tests := []struct {
		name            string
		marshaler       runtime.Marshaler
		msgs            []msg
		statusCode      int
		wantContentType string
	}{{
		name:      "encoding",
		marshaler: marshaler,
		msgs: []msg{
			{&pb.SimpleMessage{Id: "One"}, nil},
			{&pb.SimpleMessage{Id: "Two"}, nil},
		},
		statusCode:      http.StatusOK,
		wantContentType: "Custom-Content-Type",
	}, {
		name:       "empty",
		marshaler:  marshaler,
		statusCode: http.StatusOK,
	}, {
		name:            "error",
		marshaler:       marshaler,
		msgs:            []msg{{nil, status.Errorf(codes.OutOfRange, "400")}},
		statusCode:      http.StatusBadRequest,
		wantContentType: "Custom-Content-Type",
	}, {
		name:      "stream_error",
		marshaler: marshaler,
		msgs: []msg{
			{&pb.SimpleMessage{Id: "One"}, nil},
			{nil, status.Errorf(codes.OutOfRange, "400")},
		},
		statusCode:      http.StatusOK,
		wantContentType: "Custom-Content-Type",
	}, {
		name: "stream_content_type",
		marshaler: marshalerStreamContentType{
			Marshaler:               marshaler,
			CustomStreamContentType: "Stream-Content-Type",
		},
		msgs: []msg{
			{&pb.SimpleMessage{Id: "One"}, nil},
		},
		statusCode:      http.StatusOK,
		wantContentType: "Stream-Content-Type",
	}}

	newTestRecv := func(t *testing.T, msgs []msg) func() (proto.Message, error) {
		var count int
		return func() (proto.Message, error) {
			if count == len(msgs) {
				return nil, io.EOF
			} else if count > len(msgs) {
				t.Errorf("recv() called %d times for %d messages", count, len(msgs))
			}
			count++
			msg := msgs[count-1]
			return msg.pb, msg.err
		}
	}
	ctx := runtime.NewServerMetadataContext(context.Background(), runtime.ServerMetadata{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recv := newTestRecv(t, tt.msgs)
			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			resp := httptest.NewRecorder()

			runtime.ForwardResponseStream(ctx, runtime.NewServeMux(), tt.marshaler, resp, req, recv)

			w := resp.Result()
			if w.StatusCode != tt.statusCode {
				t.Errorf("StatusCode %d want %d", w.StatusCode, tt.statusCode)
			}
			if h := w.Header.Get("Transfer-Encoding"); h != "chunked" {
				t.Errorf("ForwardResponseStream missing header chunked")
			}
			body, err := io.ReadAll(w.Body)
			if err != nil {
				t.Errorf("Failed to read response body with %v", err)
			}
			w.Body.Close()
			if w.Header.Get("Content-Type") != tt.wantContentType {
				t.Errorf("Content-Type %q want %q", w.Header.Get("Content-Type"), tt.wantContentType)
			}

			var want []byte
			for _, msg := range tt.msgs {
				if msg.err != nil {
					t.Skip("checking error encodings")
				}
				b, err := tt.marshaler.Marshal(map[string]proto.Message{"result": msg.pb})
				if err != nil {
					t.Errorf("marshaler.Marshal() failed %v", err)
				}
				want = append(want, b...)
				want = append(want, "\n"...)
			}

			if string(body) != string(want) {
				t.Errorf("ForwardResponseStream() = \"%s\" want \"%s\"", body, want)
			}
		})
	}
}

func TestForwardResponseMessage(t *testing.T) {
	msg := &pb.SimpleMessage{Id: "One"}
	tests := []struct {
		name              string
		marshaler         runtime.Marshaler
		contentType       string
		frw               runtime.ForwardResponseRewriter
		getWantedResponse func(msg any) ([]byte, error)
	}{{
		name:        "standard marshaler",
		marshaler:   &runtime.JSONPb{},
		contentType: "application/json",
	}, {
		name:        "httpbody marshaler",
		marshaler:   &runtime.HTTPBodyMarshaler{&runtime.JSONPb{}},
		contentType: "application/json",
	}, {
		name:        "custom marshaler",
		marshaler:   &CustomMarshaler{&runtime.JSONPb{}},
		contentType: "Custom-Content-Type",
	}, {
		name:        "custom forward response rewriter",
		marshaler:   &runtime.JSONPb{},
		contentType: "application/json",
		frw: func(ctx context.Context, response proto.Message) (any, error) {
			return map[string]any{
				"ok":   true,
				"data": response,
			}, nil
		},
		getWantedResponse: func(msg any) ([]byte, error) {
			return new(runtime.JSONPb).Marshal(map[string]any{"ok": true, "data": msg})
		},
	}}

	ctx := runtime.NewServerMetadataContext(context.Background(), runtime.ServerMetadata{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			resp := httptest.NewRecorder()

			opts := []runtime.ServeMuxOption{}
			if tt.frw != nil {
				opts = append(opts, runtime.WithForwardResponseRewriter(tt.frw))
			}

			runtime.ForwardResponseMessage(ctx, runtime.NewServeMux(opts...), tt.marshaler, resp, req, msg)

			w := resp.Result()
			if w.StatusCode != http.StatusOK {
				t.Errorf("StatusCode %d want %d", w.StatusCode, http.StatusOK)
			}
			if h := w.Header.Get("Content-Type"); h != tt.contentType {
				t.Errorf("Content-Type %v want %v", h, tt.contentType)
			}
			body, err := io.ReadAll(w.Body)
			if err != nil {
				t.Errorf("Failed to read response body with %v", err)
			}
			w.Body.Close()

			if tt.getWantedResponse == nil {
				tt.getWantedResponse = tt.marshaler.Marshal
			}
			want, err := tt.getWantedResponse(msg)
			if err != nil {
				t.Errorf("marshaler.Marshal() failed %v", err)
			}

			if string(body) != string(want) {
				t.Errorf("ForwardResponseMessage() = \"%s\" want \"%s\"", body, want)
			}
		})
	}
}

func TestOutgoingHeaderMatcher(t *testing.T) {
	t.Parallel()
	msg := &pb.SimpleMessage{Id: "foo"}
	for _, tc := range []struct {
		name    string
		md      runtime.ServerMetadata
		headers http.Header
		matcher runtime.HeaderMatcherFunc
	}{
		{
			name: "default matcher",
			md: runtime.ServerMetadata{
				HeaderMD: metadata.Pairs(
					"foo", "bar",
					"baz", "qux",
				),
			},
			headers: http.Header{
				"Content-Type":      []string{"application/json"},
				"Grpc-Metadata-Foo": []string{"bar"},
				"Grpc-Metadata-Baz": []string{"qux"},
			},
		},
		{
			name: "custom matcher",
			md: runtime.ServerMetadata{
				HeaderMD: metadata.Pairs(
					"foo", "bar",
					"baz", "qux",
				),
			},
			headers: http.Header{
				"Content-Type": []string{"application/json"},
				"Custom-Foo":   []string{"bar"},
			},
			matcher: func(key string) (string, bool) {
				switch key {
				case "foo":
					return "custom-foo", true
				default:
					return "", false
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := runtime.NewServerMetadataContext(context.Background(), tc.md)

			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			resp := httptest.NewRecorder()

			mux := runtime.NewServeMux(
				runtime.WithOutgoingHeaderMatcher(tc.matcher),
			)
			runtime.ForwardResponseMessage(ctx, mux, &runtime.JSONPb{}, resp, req, msg)

			w := resp.Result()
			defer w.Body.Close()
			if w.StatusCode != http.StatusOK {
				t.Fatalf("StatusCode %d want %d", w.StatusCode, http.StatusOK)
			}

			if !reflect.DeepEqual(w.Header, tc.headers) {
				t.Fatalf("Header %v want %v", w.Header, tc.headers)
			}
		})
	}
}

func TestOutgoingHeaderMatcherWithContentLength(t *testing.T) {
	t.Parallel()
	msg := &pb.SimpleMessage{Id: "foo"}
	for _, tc := range []struct {
		name    string
		md      runtime.ServerMetadata
		headers http.Header
		matcher runtime.HeaderMatcherFunc
	}{
		{
			name: "default matcher",
			md: runtime.ServerMetadata{
				HeaderMD: metadata.Pairs(
					"foo", "bar",
					"baz", "qux",
				),
			},
			headers: http.Header{
				"Content-Length":    []string{"12"},
				"Content-Type":      []string{"application/json"},
				"Grpc-Metadata-Foo": []string{"bar"},
				"Grpc-Metadata-Baz": []string{"qux"},
			},
		},
		{
			name: "custom matcher",
			md: runtime.ServerMetadata{
				HeaderMD: metadata.Pairs(
					"foo", "bar",
					"baz", "qux",
				),
			},
			headers: http.Header{
				"Content-Length": []string{"12"},
				"Content-Type":   []string{"application/json"},
				"Custom-Foo":     []string{"bar"},
			},
			matcher: func(key string) (string, bool) {
				switch key {
				case "foo":
					return "custom-foo", true
				default:
					return "", false
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := runtime.NewServerMetadataContext(context.Background(), tc.md)

			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			resp := httptest.NewRecorder()

			mux := runtime.NewServeMux(
				runtime.WithOutgoingHeaderMatcher(tc.matcher),
				runtime.WithWriteContentLength(),
			)
			runtime.ForwardResponseMessage(ctx, mux, &runtime.JSONPb{}, resp, req, msg)

			w := resp.Result()
			defer w.Body.Close()
			if w.StatusCode != http.StatusOK {
				t.Fatalf("StatusCode %d want %d", w.StatusCode, http.StatusOK)
			}

			if !reflect.DeepEqual(w.Header, tc.headers) {
				t.Fatalf("Header %v want %v", w.Header, tc.headers)
			}
		})
	}
}

func TestOutgoingTrailerMatcher(t *testing.T) {
	t.Parallel()
	msg := &pb.SimpleMessage{Id: "foo"}
	for _, tc := range []struct {
		name    string
		md      runtime.ServerMetadata
		caller  http.Header
		headers http.Header
		trailer http.Header
		matcher runtime.HeaderMatcherFunc
	}{
		{
			name: "default matcher, caller accepts",
			md: runtime.ServerMetadata{
				TrailerMD: metadata.Pairs(
					"foo", "bar",
					"baz", "qux",
				),
			},
			caller: http.Header{
				"Te": []string{"trailers"},
			},
			headers: http.Header{
				"Transfer-Encoding": []string{"chunked"},
				"Content-Type":      []string{"application/json"},
				"Trailer":           []string{"Grpc-Trailer-Baz", "Grpc-Trailer-Foo"},
			},
			trailer: http.Header{
				"Grpc-Trailer-Foo": []string{"bar"},
				"Grpc-Trailer-Baz": []string{"qux"},
			},
		},
		{
			name: "default matcher, caller rejects",
			md: runtime.ServerMetadata{
				TrailerMD: metadata.Pairs(
					"foo", "bar",
					"baz", "qux",
				),
			},
			headers: http.Header{
				"Content-Length": []string{"12"},
				"Content-Type":   []string{"application/json"},
			},
		},
		{
			name: "custom matcher",
			md: runtime.ServerMetadata{
				TrailerMD: metadata.Pairs(
					"foo", "bar",
					"baz", "qux",
				),
			},
			caller: http.Header{
				"Te": []string{"trailers"},
			},
			headers: http.Header{
				"Transfer-Encoding": []string{"chunked"},
				"Content-Type":      []string{"application/json"},
				"Trailer":           []string{"Custom-Trailer-Foo"},
			},
			trailer: http.Header{
				"Custom-Trailer-Foo": []string{"bar"},
			},
			matcher: func(key string) (string, bool) {
				switch key {
				case "foo":
					return "custom-trailer-foo", true
				default:
					return "", false
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := runtime.NewServerMetadataContext(context.Background(), tc.md)

			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			req.Header = tc.caller
			resp := httptest.NewRecorder()

			mux := runtime.NewServeMux(
				runtime.WithOutgoingTrailerMatcher(tc.matcher),
				runtime.WithWriteContentLength(),
			)
			runtime.ForwardResponseMessage(ctx, mux, &runtime.JSONPb{}, resp, req, msg)

			w := resp.Result()
			_, _ = io.Copy(io.Discard, w.Body)
			defer w.Body.Close()
			if w.StatusCode != http.StatusOK {
				t.Fatalf("StatusCode %d want %d", w.StatusCode, http.StatusOK)
			}

			// Sort to the trailer headers to ensure the test is deterministic
			sort.Strings(w.Header["Trailer"])

			if !reflect.DeepEqual(w.Header, tc.headers) {
				t.Fatalf("Header %v want %v", w.Header, tc.headers)
			}

			if !reflect.DeepEqual(w.Trailer, tc.trailer) {
				t.Fatalf("Trailer %v want %v", w.Trailer, tc.trailer)
			}
		})
	}
}

// TestForwardResponseMessageHeaderAndTrailer exercises the unified propagation
// rules for metadata that is configured as both a response header and a
// trailer: each value stays in its declared location, multi-value order is
// preserved, empty values are forwarded as empty HTTP field values, and a name
// both matchers produce is resolved deterministically (header wins).
func TestForwardResponseMessageHeaderAndTrailer(t *testing.T) {
	t.Parallel()
	msg := &pb.SimpleMessage{Id: "foo"}
	for _, tc := range []struct {
		name          string
		md            runtime.ServerMetadata
		headerMatcher runtime.HeaderMatcherFunc
		headers       http.Header
		trailer       http.Header
	}{
		{
			name: "same key as header and trailer stays in its declared location",
			md: runtime.ServerMetadata{
				HeaderMD:  metadata.Pairs("foo", "h1", "foo", "h2"),
				TrailerMD: metadata.Pairs("foo", "t1", "foo", "t2"),
			},
			headers: http.Header{
				"Content-Type":      []string{"application/json"},
				"Transfer-Encoding": []string{"chunked"},
				"Trailer":           []string{"Grpc-Trailer-Foo"},
				"Grpc-Metadata-Foo": []string{"h1", "h2"},
			},
			trailer: http.Header{
				"Grpc-Trailer-Foo": []string{"t1", "t2"},
			},
		},
		{
			name: "multi-value order preserved with empty value in the middle",
			md: runtime.ServerMetadata{
				HeaderMD:  metadata.Pairs("foo", "a", "foo", "", "foo", "b"),
				TrailerMD: metadata.Pairs("foo", "x", "foo", "", "foo", "y"),
			},
			headers: http.Header{
				"Content-Type":      []string{"application/json"},
				"Transfer-Encoding": []string{"chunked"},
				"Trailer":           []string{"Grpc-Trailer-Foo"},
				"Grpc-Metadata-Foo": []string{"a", "", "b"},
			},
			trailer: http.Header{
				"Grpc-Trailer-Foo": []string{"x", "", "y"},
			},
		},
		{
			name: "metadata keys are normalized before matching",
			md: runtime.ServerMetadata{
				// Built by hand, bypassing metadata.Pairs' lower-casing.
				HeaderMD:  metadata.MD{"Foo": []string{"bar"}},
				TrailerMD: metadata.MD{"Foo": []string{"baz"}},
			},
			headers: http.Header{
				"Content-Type":      []string{"application/json"},
				"Transfer-Encoding": []string{"chunked"},
				"Trailer":           []string{"Grpc-Trailer-Foo"},
				"Grpc-Metadata-Foo": []string{"bar"},
			},
			trailer: http.Header{
				"Grpc-Trailer-Foo": []string{"baz"},
			},
		},
		{
			name: "matcher name collision resolves to the header",
			md: runtime.ServerMetadata{
				HeaderMD:  metadata.Pairs("foo", "h1"),
				TrailerMD: metadata.Pairs("foo", "t1"),
			},
			headerMatcher: func(key string) (string, bool) {
				if key == "foo" {
					return "x-thing", true
				}
				return "", false
			},
			headers: http.Header{
				"Content-Type":      []string{"application/json"},
				"Transfer-Encoding": []string{"chunked"},
				"X-Thing":           []string{"h1"},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := runtime.NewServerMetadataContext(context.Background(), tc.md)

			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			req.Header = http.Header{"Te": []string{"trailers"}}
			resp := httptest.NewRecorder()

			headerMatcher := tc.headerMatcher
			if headerMatcher == nil {
				headerMatcher = func(key string) (string, bool) {
					return runtime.MetadataHeaderPrefix + key, true
				}
			}
			mux := runtime.NewServeMux(
				runtime.WithOutgoingHeaderMatcher(headerMatcher),
				// The trailer matcher maps to the same suffix as the header
				// matcher, so a key present in both MDs is dual-configured.
				runtime.WithOutgoingTrailerMatcher(func(key string) (string, bool) {
					if tc.headerMatcher != nil {
						return tc.headerMatcher(key)
					}
					return runtime.MetadataTrailerPrefix + key, true
				}),
			)
			runtime.ForwardResponseMessage(ctx, mux, &runtime.JSONPb{}, resp, req, msg)

			w := resp.Result()
			_, _ = io.Copy(io.Discard, w.Body)
			defer w.Body.Close()
			if w.StatusCode != http.StatusOK {
				t.Fatalf("StatusCode %d want %d", w.StatusCode, http.StatusOK)
			}

			if !reflect.DeepEqual(w.Header, tc.headers) {
				t.Fatalf("Header %v want %v", w.Header, tc.headers)
			}

			if !reflect.DeepEqual(w.Trailer, tc.trailer) {
				t.Fatalf("Trailer %v want %v", w.Trailer, tc.trailer)
			}
		})
	}
}

// TestForwardResponseStreamTrailerFlush verifies that a stream forwards its
// trailer metadata on every exit -- clean end of stream and mid-stream error
// alike -- so closing the stream never leaves announced metadata pending, and
// that no trailer is announced when the client does not accept trailers.
func TestForwardResponseStreamTrailerFlush(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		msgs         []proto.Message
		err          error
		caller       http.Header
		wantTrailer  http.Header
		wantAnnounce bool
	}{
		{
			name:         "flushed at end of stream",
			msgs:         []proto.Message{&pb.SimpleMessage{Id: "One"}},
			caller:       http.Header{"Te": []string{"trailers"}},
			wantTrailer:  http.Header{"Grpc-Trailer-Foo": []string{"t1", "t2"}},
			wantAnnounce: true,
		},
		{
			name:         "flushed after mid-stream error",
			msgs:         []proto.Message{&pb.SimpleMessage{Id: "One"}},
			err:          status.Errorf(codes.OutOfRange, "boom"),
			caller:       http.Header{"Te": []string{"trailers"}},
			wantTrailer:  http.Header{"Grpc-Trailer-Foo": []string{"t1", "t2"}},
			wantAnnounce: true,
		},
		{
			name:         "flushed after immediate error",
			err:          status.Errorf(codes.OutOfRange, "boom"),
			caller:       http.Header{"Te": []string{"trailers"}},
			wantTrailer:  http.Header{"Grpc-Trailer-Foo": []string{"t1", "t2"}},
			wantAnnounce: true,
		},
		{
			name: "not announced when caller rejects trailers",
			msgs: []proto.Message{&pb.SimpleMessage{Id: "One"}},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			md := runtime.ServerMetadata{
				TrailerMD: metadata.Pairs("foo", "t1", "foo", "t2"),
			}
			ctx := runtime.NewServerMetadataContext(context.Background(), md)

			var count int
			errPending := tc.err
			recv := func() (proto.Message, error) {
				if count < len(tc.msgs) {
					count++
					return tc.msgs[count-1], nil
				}
				if errPending != nil {
					err := errPending
					errPending = nil
					return nil, err
				}
				return nil, io.EOF
			}

			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			req.Header = tc.caller
			resp := httptest.NewRecorder()

			mux := runtime.NewServeMux()
			runtime.ForwardResponseStream(ctx, mux, &runtime.JSONPb{}, resp, req, recv)

			w := resp.Result()
			_, _ = io.Copy(io.Discard, w.Body)
			defer w.Body.Close()

			announced := w.Header.Values("Trailer")
			if tc.wantAnnounce {
				if !reflect.DeepEqual(announced, []string{"Grpc-Trailer-Foo"}) {
					t.Fatalf("Trailer announcement %v want %v", announced, []string{"Grpc-Trailer-Foo"})
				}
			} else if len(announced) != 0 {
				t.Fatalf("unexpected Trailer announcement %v", announced)
			}

			if !reflect.DeepEqual(w.Trailer, tc.wantTrailer) {
				t.Fatalf("Trailer %v want %v", w.Trailer, tc.wantTrailer)
			}
		})
	}
}

// BenchmarkForwardResponseMessage measures the unary response path: the server metadata a
// backend returns is turned into response headers, the message is marshalled and written.
//
// The withMetadata arm is the one that matters for the outgoing header matcher, which runs once
// per metadata entry.
func BenchmarkForwardResponseMessage(b *testing.B) {
	mux := runtime.NewServeMux()
	marshaler := &runtime.JSONPb{}
	req := httptest.NewRequest(http.MethodPost, "/v1/echo", nil)
	msg := &pb.SimpleMessage{Id: "6EC2446F-7E89-4127-B3E6-5C05E6BECBA7"}

	plain := context.Background()
	withMetadata := runtime.NewServerMetadataContext(context.Background(), runtime.ServerMetadata{
		HeaderMD: metadata.Pairs(
			"x-request-id", "req-0123456789abcdef",
			"x-trace-id", "4bf92f3577b34da6a3ce929d0e0e4736",
			"x-tenant", "acme",
		),
	})

	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{"plain", plain},
		{"withMetadata", withMetadata},
	} {
		b.Run(tc.name, func(b *testing.B) {
			rec := httptest.NewRecorder()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runtime.ForwardResponseMessage(tc.ctx, mux, marshaler, rec, req, msg)
			}
		})
	}
}
