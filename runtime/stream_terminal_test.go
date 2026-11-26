package runtime_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/grpc-ecosystem/grpc-gateway/v2/runtime/internal/examplepb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// feedback432EventRecorder records the order in which the response status and the
// first body bytes are committed.
type feedback432EventRecorder struct {
	rec         *httptest.ResponseRecorder
	events      []string
	failOnWrite int
	writeCount  int
}

func newFeedback432EventRecorder(failOnWrite int) *feedback432EventRecorder {
	return &feedback432EventRecorder{
		rec:         httptest.NewRecorder(),
		failOnWrite: failOnWrite,
	}
}

func (w *feedback432EventRecorder) Header() http.Header {
	return w.rec.Header()
}

func (w *feedback432EventRecorder) WriteHeader(code int) {
	w.events = append(w.events, "write-header")
	w.rec.WriteHeader(code)
}

func (w *feedback432EventRecorder) Write(p []byte) (int, error) {
	w.writeCount++
	if len(w.events) == 0 || w.events[len(w.events)-1] != "write" {
		w.events = append(w.events, "write")
	}
	if w.failOnWrite > 0 && w.writeCount == w.failOnWrite {
		return 0, errors.New("write: client disconnected")
	}
	return w.rec.Write(p)
}

func (w *feedback432EventRecorder) Flush() {
	w.rec.Flush()
}

func feedback432Recv(chunks ...struct {
	msg proto.Message
	err error
}) func() (proto.Message, error) {
	i := 0
	return func() (proto.Message, error) {
		if i >= len(chunks) {
			return nil, io.EOF
		}
		chunk := chunks[i]
		i++
		return chunk.msg, chunk.err
	}
}

func feedback432TERequest(ctx context.Context) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/v1/stream", nil).WithContext(ctx)
	req.Header.Set("Te", "trailers")
	return req
}

func TestFeedback432StreamTerminalHTTPStatusMapping(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{name: "client cancellation", err: status.Error(codes.Canceled, context.Canceled.Error()), want: 499},
		{name: "context cancellation", err: context.Canceled, want: 499},
		{name: "upstream reset", err: status.Error(codes.Unavailable, "rpc error: connection reset by peer"), want: http.StatusBadGateway},
		{name: "upstream connection error", err: status.Error(codes.Unavailable, "upstream connection disconnected"), want: http.StatusBadGateway},
		{name: "unexpected EOF", err: status.Error(codes.Internal, "error reading from server: unexpected EOF"), want: http.StatusBadGateway},
		{name: "ordinary unavailable", err: status.Error(codes.Unavailable, "no healthy upstream"), want: http.StatusServiceUnavailable},
		{name: "invalid argument", err: status.Error(codes.InvalidArgument, "bad request"), want: http.StatusBadRequest},
		{name: "transport word in client error", err: status.Error(codes.InvalidArgument, "cannot disconnect an inactive session"), want: http.StatusBadRequest},
		{name: "deadline", err: context.DeadlineExceeded, want: http.StatusGatewayTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := runtime.HTTPStatusFromError(tc.err); got != tc.want {
				t.Fatalf("HTTPStatusFromError(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

func TestFeedback432ForwardResponseStreamEarlyFailure(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		err        error
		cancelCtx  bool
		wantStatus int
		wantCode   codes.Code
	}{
		{
			name:       "client cancellation",
			err:        status.Error(codes.Canceled, "canceled"),
			cancelCtx:  true,
			wantStatus: 499,
			wantCode:   codes.Canceled,
		},
		{
			name:       "raw context cancellation",
			err:        context.Canceled,
			cancelCtx:  true,
			wantStatus: 499,
			wantCode:   codes.Canceled,
		},
		{
			name:       "upstream disconnect",
			err:        status.Error(codes.Unavailable, "connection reset by peer"),
			wantStatus: http.StatusBadGateway,
			wantCode:   codes.Unavailable,
		},
		{
			name:       "raw unexpected EOF",
			err:        io.ErrUnexpectedEOF,
			wantStatus: http.StatusBadGateway,
			wantCode:   codes.Unavailable,
		},
		{
			name:       "ordinary error",
			err:        status.Error(codes.FailedPrecondition, "not ready"),
			wantStatus: http.StatusBadRequest,
			wantCode:   codes.FailedPrecondition,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			baseCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelCtx {
				cancel()
			}
			ctx := runtime.NewServerMetadataContext(baseCtx, runtime.ServerMetadata{})
			req := feedback432TERequest(ctx)
			w := newFeedback432EventRecorder(0)
			mux := runtime.NewServeMux()
			recv := feedback432Recv(struct {
				msg proto.Message
				err error
			}{nil, tc.err})

			runtime.ForwardResponseStream(ctx, mux, &runtime.JSONPb{}, w, req, recv)

			res := w.rec.Result()
			defer res.Body.Close()
			if res.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.wantStatus)
			}
			if len(w.events) < 2 || w.events[0] != "write-header" || w.events[1] != "write" {
				t.Fatalf("commit events = %v, want write-header before write", w.events)
			}
			if got := res.Trailer.Get("Grpc-Status"); got != strconv.Itoa(int(tc.wantCode)) {
				t.Fatalf("Grpc-Status trailer = %q, want %d", got, tc.wantCode)
			}
			if got := res.Header.Values("Trailer"); !feedback432ContainsString(got, "Grpc-Status") {
				t.Fatalf("Trailer announcement = %v, want Grpc-Status", got)
			}
		})
	}
}

func TestFeedback432ForwardResponseStreamPartialFailure(t *testing.T) {
	t.Parallel()

	bigStatus, err := status.New(codes.Internal, "upstream failed\ninjected-header: yes\n"+strings.Repeat("m", 6000)).WithDetails(
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{{
				Field:       "name",
				Description: strings.Repeat("非常长的错误详情-", 2000),
			}},
		},
	)
	if err != nil {
		t.Fatalf("WithDetails() error = %v", err)
	}

	ctx := runtime.NewServerMetadataContext(context.Background(), runtime.ServerMetadata{})
	req := feedback432TERequest(ctx)
	rec := httptest.NewRecorder()
	mux := runtime.NewServeMux()
	first := &pb.SimpleMessage{Id: "delivered-before-failure"}
	recv := feedback432Recv(
		struct {
			msg proto.Message
			err error
		}{first, nil},
		struct {
			msg proto.Message
			err error
		}{nil, bigStatus.Err()},
	)

	runtime.ForwardResponseStream(ctx, mux, &runtime.JSONPb{}, rec, req, recv)

	res := rec.Result()
	defer res.Body.Close()
	body := rec.Body.String()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("status after partial write = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "delivered-before-failure") {
		t.Fatalf("body %q does not contain delivered chunk", body)
	}
	if !strings.HasSuffix(body, "\n") {
		t.Fatalf("body %q must retain the stream delimiter after the error chunk", body)
	}

	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	var envelope struct {
		Error struct {
			Code    float64 `json:"code"`
			Message string  `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &envelope); err != nil {
		t.Fatalf("final stream line is not the error envelope: %v\nline=%q", err, lines[len(lines)-1])
	}
	if int(envelope.Error.Code) != int(codes.Internal) {
		t.Fatalf("body error code = %v, want %d", envelope.Error.Code, codes.Internal)
	}

	trailers := res.Trailer
	if got := trailers.Get("Grpc-Status"); got != "13" {
		t.Fatalf("Grpc-Status = %q, want 13", got)
	}
	message := trailers.Get("Grpc-Message")
	if strings.ContainsAny(message, "\r\n") {
		t.Fatalf("Grpc-Message %q contains a raw CR/LF and can break trailer framing", message)
	}
	if !strings.HasSuffix(message, "...") {
		t.Fatalf("Grpc-Message %q should be truncated with a suffix", message)
	}

	detailsBin := trailers.Get("Grpc-Status-Details-Bin")
	if detailsBin == "" {
		t.Fatal("Grpc-Status-Details-Bin trailer is empty")
	}
	if len(detailsBin) > 2<<10 {
		t.Fatalf("details trailer length = %d, want <= 2048", len(detailsBin))
	}
	rawDetails, err := base64.StdEncoding.DecodeString(detailsBin)
	if err != nil {
		t.Fatalf("truncated details trailer is not valid base64: %v", err)
	}
	var detailsStatus statuspb.Status
	if err := proto.Unmarshal(rawDetails, &detailsStatus); err != nil {
		t.Fatalf("truncated details trailer is not a valid google.rpc.Status: %v", err)
	}
	if detailsStatus.Code != int32(codes.Internal) || len(detailsStatus.Details) != 1 {
		t.Fatalf("truncated details status = code %d, %d details; want code %d and one retained detail", detailsStatus.Code, len(detailsStatus.Details), codes.Internal)
	}
	if detailsStatus.Details[0].TypeUrl == "" {
		t.Fatal("truncated detail did not retain its type URL")
	}
}

func TestFeedback432ForwardResponseStreamLateWriteFailure(t *testing.T) {
	t.Parallel()

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := runtime.NewServerMetadataContext(baseCtx, runtime.ServerMetadata{})
	req := feedback432TERequest(ctx)
	// The first Write is the data chunk; fail the delimiter write late.
	w := newFeedback432EventRecorder(2)
	mux := runtime.NewServeMux()
	recv := feedback432Recv(struct {
		msg proto.Message
		err error
	}{&pb.SimpleMessage{Id: "committed"}, nil})

	done := make(chan struct{})
	go func() {
		defer close(done)
		runtime.ForwardResponseStream(ctx, mux, &runtime.JSONPb{}, w, req, recv)
	}()
	<-done

	if w.rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want committed 200", w.rec.Code)
	}
	headerWrites := 0
	for _, event := range w.events {
		if event == "write-header" {
			headerWrites++
		}
	}
	if headerWrites != 0 {
		t.Fatalf("explicit WriteHeader calls = %d, events=%v; the first Write should commit the status once", headerWrites, w.events)
	}
	res := w.rec.Result()
	defer res.Body.Close()
	if got := res.Trailer.Get("Grpc-Status"); got != strconv.Itoa(int(codes.Canceled)) {
		t.Fatalf("late write failure Grpc-Status = %q, want %d", got, codes.Canceled)
	}
}

func TestFeedback432HTTPStreamErrorTrailers(t *testing.T) {
	t.Parallel()

	st, err := status.New(codes.ResourceExhausted, strings.Repeat("x", 20000)).WithDetails(
		&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Description: strings.Repeat("d", 50000)}}},
	)
	if err != nil {
		t.Fatalf("WithDetails() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	mux := runtime.NewServeMux()
	runtime.HTTPStreamError(context.Background(), mux, &runtime.JSONPb{}, rec, req, st.Err())

	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("HTTPStreamError status = %d, want historical 200", res.StatusCode)
	}
	if got := res.Trailer.Get("Grpc-Status"); got != strconv.Itoa(int(codes.ResourceExhausted)) {
		t.Fatalf("Grpc-Status = %q, want %d", got, codes.ResourceExhausted)
	}
	if got := len(res.Trailer.Get("Grpc-Message")); got > 1<<10 {
		t.Fatalf("message trailer length = %d, want <= 1024", got)
	}
	if got := len(res.Trailer.Get("Grpc-Status-Details-Bin")); got > 2<<10 {
		t.Fatalf("details trailer length = %d, want <= 2048", got)
	}
}

func TestFeedback432OversizedTrailerOverRealHTTP(t *testing.T) {
	t.Parallel()

	st, err := status.New(codes.Internal, strings.Repeat("m", 20000)).WithDetails(
		&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Description: strings.Repeat("d", 50000)}}},
	)
	if err != nil {
		t.Fatalf("WithDetails() error = %v", err)
	}
	mux := runtime.NewServeMux()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtime.HTTPStreamError(r.Context(), mux, &runtime.JSONPb{}, w, r, st.Err())
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET error = %v", err)
	}
	defer res.Body.Close()
	if _, err := io.ReadAll(res.Body); err != nil {
		t.Fatalf("reading oversized trailer response failed: %v", err)
	}
	if got := res.Trailer.Get("Grpc-Status"); got != strconv.Itoa(int(codes.Internal)) {
		t.Fatalf("Grpc-Status = %q, want %d", got, codes.Internal)
	}
	if got := len(res.Trailer.Get("Grpc-Message")); got > 1<<10 {
		t.Fatalf("message trailer length = %d, want <= 1024", got)
	}
	if got := len(res.Trailer.Get("Grpc-Status-Details-Bin")); got > 2<<10 {
		t.Fatalf("details trailer length = %d, want <= 2048", got)
	}
}

func feedback432ContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
