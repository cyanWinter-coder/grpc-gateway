package runtime_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/grpc-ecosystem/grpc-gateway/v2/runtime/internal/examplepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMarshalerForRequest(t *testing.T) {
	ctx := context.Background()
	r, err := http.NewRequestWithContext(ctx, "GET", "http://example.com", nil)
	if err != nil {
		t.Fatalf(`http.NewRequest("GET", "http://example.com", nil) failed with %v; want success`, err)
	}

	mux := runtime.NewServeMux()

	r.Header.Set("Accept", "application/x-out")
	r.Header.Set("Content-Type", "application/x-in")
	in, out := runtime.MarshalerForRequest(mux, r)
	if _, ok := in.(*runtime.HTTPBodyMarshaler); !ok {
		t.Errorf("in = %#v; want a runtime.HTTPBodyMarshaler", in)
	}
	if _, ok := out.(*runtime.HTTPBodyMarshaler); !ok {
		t.Errorf("out = %#v; want a runtime.HTTPBodyMarshaler", in)
	}

	marshalers := []dummyMarshaler{0, 1, 2}
	specs := []struct {
		opt runtime.ServeMuxOption

		wantIn  runtime.Marshaler
		wantOut runtime.Marshaler
	}{
		// The option with wildcard overwrites the default configuration
		{
			opt:     runtime.WithMarshalerOption(runtime.MIMEWildcard, &marshalers[0]),
			wantIn:  &marshalers[0],
			wantOut: &marshalers[0],
		},
		// You can specify a marshaler for a specific MIME type.
		// The output marshaler follows the input one unless specified.
		{
			opt:     runtime.WithMarshalerOption("application/x-in", &marshalers[1]),
			wantIn:  &marshalers[1],
			wantOut: &marshalers[1],
		},
		// You can also separately specify an output marshaler
		{
			opt:     runtime.WithMarshalerOption("application/x-out", &marshalers[2]),
			wantIn:  &marshalers[1],
			wantOut: &marshalers[2],
		},
	}
	for i, spec := range specs {
		var opts []runtime.ServeMuxOption
		for _, s := range specs[:i+1] {
			opts = append(opts, s.opt)
		}
		mux = runtime.NewServeMux(opts...)

		in, out = runtime.MarshalerForRequest(mux, r)
		if got, want := in, spec.wantIn; got != want {
			t.Errorf("in = %#v; want %#v", got, want)
		}
		if got, want := out, spec.wantOut; got != want {
			t.Errorf("out = %#v; want %#v", got, want)
		}
	}

	r.Header.Set("Content-Type", "application/x-in; charset=UTF-8")
	in, out = runtime.MarshalerForRequest(mux, r)
	if got, want := in, &marshalers[1]; got != want {
		t.Errorf("in = %#v; want %#v", got, want)
	}
	if got, want := out, &marshalers[2]; got != want {
		t.Errorf("out = %#v; want %#v", got, want)
	}

	r.Header.Set("Content-Type", "application/x-another")
	r.Header.Set("Accept", "application/x-another")
	in, out = runtime.MarshalerForRequest(mux, r)
	if got, want := in, &marshalers[0]; got != want {
		t.Errorf("in = %#v; want %#v", got, want)
	}
	if got, want := out, &marshalers[0]; got != want {
		t.Errorf("out = %#v; want %#v", got, want)
	}
}

type dummyMarshaler int

func (dummyMarshaler) ContentType(_ interface{}) string { return "" }
func (dummyMarshaler) Marshal(interface{}) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (dummyMarshaler) Unmarshal([]byte, interface{}) error {
	return errors.New("not implemented")
}

func (dummyMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return dummyDecoder{}
}
func (dummyMarshaler) NewEncoder(w io.Writer) runtime.Encoder {
	return dummyEncoder{}
}

func (m dummyMarshaler) GoString() string {
	return fmt.Sprintf("dummyMarshaler(%d)", m)
}

type dummyDecoder struct{}

func (dummyDecoder) Decode(interface{}) error {
	return errors.New("not implemented")
}

type dummyEncoder struct{}

func (dummyEncoder) Encode(interface{}) error {
	return errors.New("not implemented")
}

// BenchmarkMarshalerForRequest covers the marshaller lookup every generated handler performs
// once per request, and the error paths perform again.
//
// The Content-Type is the bare media type clients send, which is also exactly how marshallers
// are registered, so this is the case worth keeping cheap.
func BenchmarkMarshalerForRequest(b *testing.B) {
	// A marshaller registered under the concrete media type, as the customisation examples in
	// the documentation do. The default registry holds only MIMEWildcard, in which case the
	// lookups below miss and the Content-Type still has to be parsed.
	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/json", &runtime.JSONPb{}))
	req, err := http.NewRequest(http.MethodPost, "https://example.com/v1/echo", nil)
	if err != nil {
		b.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inbound, outbound := runtime.MarshalerForRequest(mux, req)
		if inbound == nil || outbound == nil {
			b.Fatal("got a nil marshaler")
		}
	}
}

// BenchmarkMarshalerForRequestWithParams keeps the path that still has to parse measured: a
// Content-Type carrying parameters is not a registry key, so it falls through to
// mime.ParseMediaType.
func BenchmarkMarshalerForRequestWithParams(b *testing.B) {
	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/json", &runtime.JSONPb{}))
	req, err := http.NewRequest(http.MethodPost, "https://example.com/v1/echo", nil)
	if err != nil {
		b.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inbound, outbound := runtime.MarshalerForRequest(mux, req)
		if inbound == nil || outbound == nil {
			b.Fatal("got a nil marshaler")
		}
	}
}

// TestMarshalerRegistryHotRegistration covers registering and removing media
// types after the mux is serving: a registration applies to the next request,
// a removal applies to the request after it, and neither disturbs an in-flight
// request.
func TestMarshalerRegistryHotRegistration(t *testing.T) {
	mux := runtime.NewServeMux()
	req := newMarshalerRequest("application/x-msgpack", "")

	if err := mux.RegisterMarshaler("application/x-msgpack", newRecordingMarshaler()); err != nil {
		t.Fatalf("RegisterMarshaler() failed with %v; want success", err)
	}
	in, out := runtime.MarshalerForRequest(mux, req)
	if _, ok := in.(*recordingMarshaler); !ok {
		t.Fatalf("in = %#v; want a *recordingMarshaler", in)
	}
	if _, ok := out.(*recordingMarshaler); !ok {
		t.Fatalf("out = %#v; want a *recordingMarshaler", out)
	}

	if !mux.UnregisterMarshaler("application/x-msgpack") {
		t.Fatal("UnregisterMarshaler(application/x-msgpack) = false; want true")
	}
	if mux.UnregisterMarshaler("application/x-msgpack") {
		t.Fatal("second UnregisterMarshaler(application/x-msgpack) = true; want false")
	}
	in, out = runtime.MarshalerForRequest(mux, req)
	if _, ok := in.(*runtime.HTTPBodyMarshaler); !ok {
		t.Errorf("in = %#v; want the default runtime.HTTPBodyMarshaler after removal", in)
	}
	if out != in {
		t.Errorf("out = %#v; want the inbound marshaler %#v", out, in)
	}
}

func TestMarshalerRegistryRegisterErrors(t *testing.T) {
	mux := runtime.NewServeMux()
	if err := mux.RegisterMarshaler("", newRecordingMarshaler()); err == nil {
		t.Error("RegisterMarshaler(\"\", ...) = nil; want an error")
	}
	if err := mux.RegisterMarshaler("application/x-nil", nil); err == nil {
		t.Error("RegisterMarshaler(..., nil) = nil; want an error")
	}
	req := newMarshalerRequest("application/x-nil", "")
	in, _ := runtime.MarshalerForRequest(mux, req)
	if _, ok := in.(*runtime.HTTPBodyMarshaler); !ok {
		t.Errorf("in = %#v; want the default marshaler after a failed registration", in)
	}
}

// TestMarshalerForRequestSnapshotIsPerRequest pins the registry snapshot when a
// request enters ServeHTTP, so the handler sees the same registry for
// negotiation and error fallback even if the registry changes mid-request.
func TestMarshalerForRequestSnapshotIsPerRequest(t *testing.T) {
	old := &failingMarshaler{contentType: "application/x-old"}
	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/x-old", old))

	var (
		inSnapshot   runtime.Marshaler
		outSnapshot  runtime.Marshaler
		errSnapshot  runtime.Marshaler
		errFallback  runtime.Marshaler
		bodySnapshot []byte
	)
	entered := make(chan struct{})
	release := make(chan struct{})
	mux.HandlePath(http.MethodPost, "/snapshot", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		inbound, outbound := runtime.MarshalerForRequest(mux, r)
		inSnapshot, outSnapshot = inbound, outbound
		close(entered)

		// Mutate the registry while this request is in flight. Registering a
		// new type and replacing the wildcard must not affect this request:
		// its error fallback would otherwise encode {} via the new wildcard.
		if err := mux.RegisterMarshaler("application/x-old", newRecordingMarshaler()); err != nil {
			t.Errorf("RegisterMarshaler() failed with %v", err)
		}
		if err := mux.RegisterMarshaler("application/x-new", newRecordingMarshaler()); err != nil {
			t.Errorf("RegisterMarshaler() failed with %v", err)
		}
		if err := mux.RegisterMarshaler(runtime.MIMEWildcard, newRecordingMarshaler()); err != nil {
			t.Errorf("RegisterMarshaler() failed with %v", err)
		}
		<-release

		// The handler's context carries the snapshot; errors raised here must
		// be encoded with the same registry the request entered with.
		inAgain, outAgain := runtime.MarshalerForRequest(mux, r)
		if inAgain != inbound || outAgain != outbound {
			t.Errorf("MarshalerForRequest() mid-request = (%#v, %#v); want (%#v, %#v)", inAgain, outAgain, inbound, outbound)
		}
		errSnapshot = inbound
		errFallback = outbound

		rec := httptest.NewRecorder()
		runtime.HTTPError(r.Context(), mux, outbound, rec, r, status.Error(codes.InvalidArgument, "boom"))
		bodySnapshot = rec.Body.Bytes()
	})

	serveErr := make(chan error, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/snapshot", nil)
		req.Header.Set("Content-Type", "application/x-old")
		req.Header.Set("Accept", "application/x-old")
		mux.ServeHTTP(httptest.NewRecorder(), req)
		serveErr <- nil
	}()

	<-entered
	// While the first request is blocked in the handler, new requests already
	// see the updated registry.
	fresh := newMarshalerRequest("application/x-new", "")
	in, out := runtime.MarshalerForRequest(mux, fresh)
	if _, ok := in.(*recordingMarshaler); !ok {
		t.Errorf("new request in = %#v; want a *recordingMarshaler from the new registration", in)
	}
	if _, ok := out.(*recordingMarshaler); !ok {
		t.Errorf("new request out = %#v; want a *recordingMarshaler from the new registration", out)
	}
	close(release)
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}

	if inSnapshot != outSnapshot {
		t.Errorf("in = %#v, out = %#v; want the same marshaler", inSnapshot, outSnapshot)
	}
	if _, ok := inSnapshot.(*failingMarshaler); !ok {
		t.Fatalf("in-flight request in = %#v; want the marshaler registered before it entered", inSnapshot)
	}
	if errSnapshot == nil || errFallback == nil {
		t.Fatal("handler did not record the mid-request marshalers")
	}
	// The dummy marshaler always fails, so DefaultHTTPErrorHandler falls back
	// within the pinned snapshot: the wildcard registered at entry was the
	// default HTTPBodyMarshaler, not the dummy registered mid-request.
	if !bytes.Contains(bodySnapshot, []byte(`"code":3`)) {
		t.Errorf("error body = %q; want the status encoded by the snapshot's fallback", bodySnapshot)
	}
	if bytes.Contains(bodySnapshot, []byte("not implemented")) {
		t.Errorf("error body = %q; want a successful fallback encoding", bodySnapshot)
	}
}

// TestMarshalerForRequestSnapshotRemoval covers the other direction: a media
// type removed after the request entered stays available to that request.
func TestMarshalerForRequestSnapshotRemoval(t *testing.T) {
	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/x-removed", newRecordingMarshaler()))
	var inSnapshot runtime.Marshaler
	entered := make(chan struct{})
	release := make(chan struct{})
	mux.HandlePath(http.MethodPost, "/removal", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		inSnapshot, _ = runtime.MarshalerForRequest(mux, r)
		close(entered)
		if !mux.UnregisterMarshaler("application/x-removed") {
			t.Error("UnregisterMarshaler(application/x-removed) = false; want true")
		}
		<-release
	})
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/removal", nil)
		req.Header.Set("Content-Type", "application/x-removed")
		mux.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-entered
	if _, ok := inSnapshot.(*recordingMarshaler); !ok {
		t.Errorf("in-flight request in = %#v; want the marshaler that was registered at entry", inSnapshot)
	}
	close(release)

	mux2 := runtime.NewServeMux(runtime.WithMarshalerOption("application/x-removed", newRecordingMarshaler()))
	if !mux2.UnregisterMarshaler("application/x-removed") {
		t.Fatal("UnregisterMarshaler() = false; want true")
	}
	late := newMarshalerRequest("application/x-removed", "")
	in, _ := runtime.MarshalerForRequest(mux2, late)
	if _, ok := in.(*recordingMarshaler); ok {
		t.Errorf("next request in = %#v; want the removed marshaler to be gone", in)
	}
}

// TestMarshalerForRequestAcceptOrdering covers deterministic Accept handling:
// quality wins over header order, equal qualities keep their order, and a
// wildcard does not override an exact match or the inbound marshaler.
func TestMarshalerForRequestAcceptOrdering(t *testing.T) {
	first := newRecordingMarshaler()
	second := newRecordingMarshaler()
	third := newRecordingMarshaler()
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption("application/x-first", first),
		runtime.WithMarshalerOption("application/x-second", second),
		runtime.WithMarshalerOption("application/x-third", third),
	)

	for _, tc := range []struct {
		name   string
		accept []string
		want   runtime.Marshaler
	}{
		{
			name:   "single exact value",
			accept: []string{"application/x-second"},
			want:   second,
		},
		{
			name:   "quality wins over order",
			accept: []string{"application/x-first;q=0.5, application/x-second;q=0.9"},
			want:   second,
		},
		{
			name:   "equal quality keeps header order",
			accept: []string{"application/x-second, application/x-first"},
			want:   second,
		},
		{
			name:   "later header wins when its quality is higher",
			accept: []string{"application/x-first;q=0.1", "application/x-third;q=0.8"},
			want:   third,
		},
		{
			name:   "zero quality is skipped",
			accept: []string{"application/x-first;q=0, application/x-third"},
			want:   third,
		},
		{
			name:   "exact match beats wildcard",
			accept: []string{"*/*, application/x-third"},
			want:   third,
		},
		{
			name:   "params are ignored when matching the media type",
			accept: []string{"application/x-second; charset=utf-8"},
			want:   second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := newMarshalerRequest("", "")
			req.Header[acceptHeaderKey()] = tc.accept
			_, out := runtime.MarshalerForRequest(mux, req)
			if out != tc.want {
				t.Errorf("out = %#v; want %#v", out, tc.want)
			}
		})
	}
}

// TestMarshalerForRequestFallbackOrder covers the inbound fallback chain:
// exact key, parsed media type, MIMEWildcard, then built-in JSON.
func TestMarshalerForRequestFallbackOrder(t *testing.T) {
	mux := runtime.NewServeMux()
	req := newMarshalerRequest("application/x-nothing", "")
	in, out := runtime.MarshalerForRequest(mux, req)
	if _, ok := in.(*runtime.HTTPBodyMarshaler); !ok {
		t.Errorf("in = %#v; want the default MIMEWildcard marshaler", in)
	}
	if out != in {
		t.Errorf("out = %#v; want the inbound marshaler %#v", out, in)
	}

	if !mux.UnregisterMarshaler(runtime.MIMEWildcard) {
		t.Fatal("UnregisterMarshaler(MIMEWildcard) = false; want true")
	}
	in, out = runtime.MarshalerForRequest(mux, req)
	if in == nil || out == nil {
		t.Fatalf("in = %#v, out = %#v; want the built-in JSON fallback", in, out)
	}
	if _, ok := in.(*runtime.HTTPBodyMarshaler); !ok {
		t.Errorf("in = %#v; want the built-in JSON marshaler when no wildcard is registered", in)
	}
	if out != in {
		t.Errorf("out = %#v; want the inbound marshaler %#v", out, in)
	}
}

// failingMarshaler always fails to encode and records its every call.
type failingMarshaler struct {
	contentType string
	mu          sync.Mutex
	calls       int
}

func (m *failingMarshaler) ContentType(interface{}) string { return m.contentType }
func (m *failingMarshaler) Marshal(interface{}) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return nil, errors.New("encode failed")
}
func (m *failingMarshaler) Unmarshal([]byte, interface{}) error {
	return errors.New("not implemented")
}
func (m *failingMarshaler) NewDecoder(io.Reader) runtime.Decoder { return dummyDecoder{} }
func (m *failingMarshaler) NewEncoder(io.Writer) runtime.Encoder { return dummyEncoder{} }

func (m *failingMarshaler) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// TestMarshalerFallbackOnEncodeFailure checks that a preferred marshaler that
// cannot encode a message falls back to a compatible marshaler from the same
// registry snapshot, and that the response advertises the fallback's type.
func TestMarshalerFallbackOnEncodeFailure(t *testing.T) {
	bad := &failingMarshaler{contentType: "application/x-bad"}
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption("application/x-bad", bad),
		// Removing the wildcard forces the built-in JSON fallback, which is
		// deterministic and not affected by later registrations.
	)
	if !mux.UnregisterMarshaler(runtime.MIMEWildcard) {
		t.Fatal("UnregisterMarshaler(MIMEWildcard) = false; want true")
	}

	ctx := runtime.NewServerMetadataContext(context.Background(), runtime.ServerMetadata{})
	req := httptest.NewRequest(http.MethodGet, "/v1/echo", nil)
	w := httptest.NewRecorder()
	msg := &pb.SimpleMessage{Id: "fallback"}
	runtime.ForwardResponseMessage(ctx, mux, bad, w, req, msg)

	if bad.callCount() != 1 {
		t.Errorf("bad marshaler called %d times; want exactly 1 attempt before falling back", bad.callCount())
	}
	if w.Code != http.StatusOK {
		t.Errorf("w.Code = %d; want %d", w.Code, http.StatusOK)
	}
	if got, want := w.Header().Get("Content-Type"), "application/json"; got != want {
		t.Errorf("Content-Type = %q; want %q", got, want)
	}
	var got pb.SimpleMessage
	if err := (&runtime.JSONPb{}).Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("fallback body is not valid JSON: %v (%q)", err, w.Body.String())
	}
	if got.Id != msg.Id {
		t.Errorf("fallback body id = %q; want %q", got.Id, msg.Id)
	}
}

// TestMarshalerFallbackOnErrorEncodeFailure checks the error path uses the same
// snapshot-local fallback as the success path.
func TestMarshalerFallbackOnErrorEncodeFailure(t *testing.T) {
	bad := &failingMarshaler{contentType: "application/x-bad"}
	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/x-bad", bad))
	if !mux.UnregisterMarshaler(runtime.MIMEWildcard) {
		t.Fatal("UnregisterMarshaler(MIMEWildcard) = false; want true")
	}

	ctx := context.Background()
	req := httptest.NewRequest(http.MethodGet, "/v1/echo", nil)
	w := httptest.NewRecorder()
	runtime.HTTPError(ctx, mux, bad, w, req, status.Error(codes.NotFound, "missing"))

	if got, want := w.Code, http.StatusNotFound; got != want {
		t.Errorf("w.Code = %d; want %d", got, want)
	}
	if got, want := w.Header().Get("Content-Type"), "application/json"; got != want {
		t.Errorf("Content-Type = %q; want %q", got, want)
	}
	if !strings.Contains(w.Body.String(), `"message":"missing"`) {
		t.Errorf("body = %q; want the status encoded by the fallback", w.Body.String())
	}
}

// TestMarshalerFallbackDoesNotUseLaterRegistration verifies fallback selection
// is snapshot-local: a type registered after the request entered must not be
// used to encode a body whose negotiated marshaler failed.
func TestMarshalerFallbackDoesNotUseLaterRegistration(t *testing.T) {
	bad := &failingMarshaler{contentType: "application/x-bad"}
	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/x-bad", bad))
	if !mux.UnregisterMarshaler(runtime.MIMEWildcard) {
		t.Fatal("UnregisterMarshaler(MIMEWildcard) = false; want true")
	}

	late := newRecordingMarshaler()
	var body []byte
	entered := make(chan struct{})
	mux.HandlePath(http.MethodGet, "/late", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		_, outbound := runtime.MarshalerForRequest(mux, r)
		close(entered)
		if err := mux.RegisterMarshaler("application/json", late); err != nil {
			t.Errorf("RegisterMarshaler() failed with %v", err)
		}
		rec := httptest.NewRecorder()
		runtime.HTTPError(r.Context(), mux, outbound, rec, r, status.Error(codes.Internal, "late"))
		body = rec.Body.Bytes()
	})
	req := httptest.NewRequest(http.MethodGet, "/late", nil)
	req.Header.Set("Content-Type", "application/x-bad")
	req.Header.Set("Accept", "application/x-bad")
	mux.ServeHTTP(httptest.NewRecorder(), req)
	<-entered

	if bytes.Contains(body, []byte("not implemented")) {
		t.Fatalf("body = %q; the fallback used the late dummy registration", body)
	}
	if !bytes.Contains(body, []byte(`"code":13`)) {
		t.Errorf("body = %q; want the status encoded by the built-in JSON fallback", body)
	}
}

func TestMarshalerRegistryConcurrentAccess(t *testing.T) {
	mux := runtime.NewServeMux()
	req := newMarshalerRequest("application/x-race", "")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mime := fmt.Sprintf("application/x-race-%d", i%3)
			for j := 0; j < 200; j++ {
				if err := mux.RegisterMarshaler(mime, newRecordingMarshaler()); err != nil {
					t.Errorf("RegisterMarshaler() failed with %v", err)
					return
				}
				in, out := runtime.MarshalerForRequest(mux, req)
				if in == nil || out == nil {
					t.Error("got a nil marshaler while the registry was changing")
					return
				}
				if j%2 == 0 {
					mux.UnregisterMarshaler(mime)
				}
			}
		}(i)
	}
	wg.Wait()
}

func newMarshalerRequest(contentType, accept string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/echo", nil)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	return req
}

func acceptHeaderKey() string { return http.CanonicalHeaderKey("Accept") }

// recordingMarshaler is a Marshaler whose identity is what tests compare; it
// always encodes an empty JSON object.
type recordingMarshaler struct{}

func newRecordingMarshaler() *recordingMarshaler { return &recordingMarshaler{} }

func (*recordingMarshaler) ContentType(interface{}) string { return "application/x-recording" }
func (*recordingMarshaler) Marshal(interface{}) ([]byte, error) {
	return []byte(`{}`), nil
}
func (*recordingMarshaler) Unmarshal([]byte, interface{}) error { return nil }
func (*recordingMarshaler) NewDecoder(io.Reader) runtime.Decoder {
	return dummyDecoder{}
}
func (*recordingMarshaler) NewEncoder(io.Writer) runtime.Encoder {
	return dummyEncoder{}
}

// TestMarshalerFallbackOnStream checks the streaming path falls back within the
// request's snapshot for both response chunks and the terminating error chunk.
func TestMarshalerFallbackOnStream(t *testing.T) {
	bad := &failingMarshaler{contentType: "application/x-bad"}
	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/x-bad", bad))
	if !mux.UnregisterMarshaler(runtime.MIMEWildcard) {
		t.Fatal("UnregisterMarshaler(MIMEWildcard) = false; want true")
	}

	ctx := runtime.NewServerMetadataContext(context.Background(), runtime.ServerMetadata{})
	req := httptest.NewRequest(http.MethodGet, "/v1/stream", nil)
	resp := httptest.NewRecorder()
	msgs := []proto.Message{
		&pb.SimpleMessage{Id: "one"},
		&pb.SimpleMessage{Id: "two"},
	}
	var i int
	recv := func() (proto.Message, error) {
		if i == len(msgs) {
			return nil, io.EOF
		}
		msg := msgs[i]
		i++
		return msg, nil
	}
	runtime.ForwardResponseStream(ctx, mux, bad, resp, req, recv)

	w := resp.Result()
	if got, want := w.Header.Get("Content-Type"), "application/json"; got != want {
		t.Errorf("Content-Type = %q; want %q", got, want)
	}
	body, err := io.ReadAll(w.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"one"`, `"two"`} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("body = %q; want it to contain %s", body, want)
		}
	}
	if bad.callCount() != len(msgs) {
		t.Errorf("failing marshaler called %d times; want %d reasoning attempts", bad.callCount(), len(msgs))
	}
}

// TestMarshalerFallbackOnStreamError checks the stream error chunk, which is
// written after the header when the first recv fails, also uses the fallback.
func TestMarshalerFallbackOnStreamError(t *testing.T) {
	bad := &failingMarshaler{contentType: "application/x-bad"}
	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/x-bad", bad))
	if !mux.UnregisterMarshaler(runtime.MIMEWildcard) {
		t.Fatal("UnregisterMarshaler(MIMEWildcard) = false; want true")
	}

	ctx := runtime.NewServerMetadataContext(context.Background(), runtime.ServerMetadata{})
	req := httptest.NewRequest(http.MethodGet, "/v1/stream", nil)
	resp := httptest.NewRecorder()
	runtime.ForwardResponseStream(ctx, mux, bad, resp, req, func() (proto.Message, error) {
		return nil, status.Error(codes.OutOfRange, "stream failed")
	})

	w := resp.Result()
	if got, want := w.StatusCode, http.StatusBadRequest; got != want {
		t.Errorf("StatusCode = %d; want %d", got, want)
	}
	body, err := io.ReadAll(w.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"stream failed"`)) {
		t.Errorf("body = %q; want the error encoded by the fallback", body)
	}
}

// TestMarshalerRegistryBuiltinTypesKeepsProtoAndJSONCompatible pins the
// behaviour of the built-in proto and JSON marshalers when registered by their
// conventional media types: each request negotiates its own codec and both
// directions resolve to the registered instance.
func TestMarshalerRegistryBuiltinTypesKeepsProtoAndJSONCompatible(t *testing.T) {
	protoMarshaller := &runtime.ProtoMarshaller{}
	jsonMarshaller := &runtime.JSONBuiltin{}
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption("application/x-protobuf", protoMarshaller),
		runtime.WithMarshalerOption("application/json", jsonMarshaller),
	)

	for _, tc := range []struct {
		mediaType string
		want      runtime.Marshaler
	}{
		{mediaType: "application/x-protobuf", want: protoMarshaller},
		{mediaType: "application/json", want: jsonMarshaller},
	} {
		req := newMarshalerRequest(tc.mediaType, tc.mediaType)
		in, out := runtime.MarshalerForRequest(mux, req)
		if in != tc.want {
			t.Errorf("%s: in = %#v; want %#v", tc.mediaType, in, tc.want)
		}
		if out != tc.want {
			t.Errorf("%s: out = %#v; want %#v", tc.mediaType, out, tc.want)
		}
	}

	// A JSON request with parameters still resolves to the registered JSON
	// marshaler rather than falling through to the wildcard default.
	req := newMarshalerRequest("application/json; charset=utf-8", "")
	in, _ := runtime.MarshalerForRequest(mux, req)
	if in != jsonMarshaller {
		t.Errorf("in = %#v; want %#v", in, jsonMarshaller)
	}

	// The proto marshaler round-trips a message through the registry lookup.
	body, err := protoMarshaller.Marshal(&pb.SimpleMessage{Id: "proto"})
	if err != nil {
		t.Fatalf("ProtoMarshaller.Marshal() failed with %v", err)
	}
	var got pb.SimpleMessage
	if err := protoMarshaller.Unmarshal(body, &got); err != nil {
		t.Fatalf("ProtoMarshaller.Unmarshal() failed with %v", err)
	}
	if got.Id != "proto" {
		t.Errorf("decoded id = %q; want %q", got.Id, "proto")
	}
}
