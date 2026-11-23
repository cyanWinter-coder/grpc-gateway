package runtime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/grpc-ecosystem/grpc-gateway/v2/runtime/internal/examplepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// blockableWriter is an http.ResponseWriter whose Write and Flush block until
// release is closed or its context is canceled, simulating a slow or
// disconnected HTTP client.
type blockableWriter struct {
	header      http.Header
	mu          sync.Mutex
	body        bytes.Buffer
	written     atomic.Int64
	writeCalls  atomic.Int64
	flushCalls  atomic.Int64
	writeErr    error
	release     chan struct{}
	ctx         context.Context
	releaseOnce sync.Once
}

func newBlockableWriter(ctx context.Context) *blockableWriter {
	return &blockableWriter{
		header:  http.Header{},
		release: make(chan struct{}),
		ctx:     ctx,
	}
}

func (b *blockableWriter) Header() http.Header { return b.header }

func (b *blockableWriter) Write(p []byte) (int, error) {
	b.writeCalls.Add(1)
	if b.writeErr != nil {
		return 0, b.writeErr
	}
	select {
	case <-b.release:
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	}
	b.mu.Lock()
	b.body.Write(p)
	b.mu.Unlock()
	b.written.Add(int64(len(p)))
	return len(p), nil
}

func (b *blockableWriter) WriteHeader(int) {}

func (b *blockableWriter) Flush() {
	b.flushCalls.Add(1)
	select {
	case <-b.release:
	case <-b.ctx.Done():
	}
}

func (b *blockableWriter) unblock() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func (b *blockableWriter) snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.body.Bytes()
}

// fixedSizeMarshaler marshals every message into a fixed-size payload so the
// number of buffered frames maps directly onto buffered bytes.
type fixedSizeMarshaler struct {
	size int
}

func (m *fixedSizeMarshaler) Marshal(v interface{}) ([]byte, error) {
	return bytes.Repeat([]byte("x"), m.size), nil
}

func (m *fixedSizeMarshaler) Unmarshal(data []byte, v interface{}) error { return nil }
func (m *fixedSizeMarshaler) NewDecoder(r io.Reader) Decoder             { return nil }
func (m *fixedSizeMarshaler) NewEncoder(w io.Writer) Encoder             { return nil }
func (m *fixedSizeMarshaler) ContentType(v interface{}) string           { return "application/json" }
func (m *fixedSizeMarshaler) Delimiter() []byte                          { return []byte("\n") }

// waitFor polls cond until it returns true or the deadline elapses.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// streamTestRecv returns a recv function serving msgs forever (or until EOF
// when finite is true) and counts every call.
func countingRecv(t *testing.T, total int, finite bool) (func() (proto.Message, error), *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	return func() (proto.Message, error) {
		n := calls.Add(1)
		if finite && int(n) > total {
			return nil, io.EOF
		}
		return &pb.SimpleMessage{Id: "x"}, nil
	}, &calls
}

// TestBoundedStreamQueueBackpressure verifies Push blocks once the buffered
// payload exceeds the byte limit and resumes after Pop drains frames.
func TestBoundedStreamQueueBackpressure(t *testing.T) {
	const maxBytes = 10
	q := newBoundedStreamQueue(maxBytes)
	ctx := context.Background()

	// The first frame is always accepted even when larger than the limit so an
	// oversized message cannot deadlock the stream.
	if err := q.Push(ctx, streamFrame{data: make([]byte, maxBytes*2)}); err != nil {
		t.Fatalf("first oversized Push: %v", err)
	}
	// The queue is now over the limit, so the next Push must block.
	pushed := make(chan error, 1)
	go func() { pushed <- q.Push(ctx, streamFrame{data: []byte("ab")}) }()
	select {
	case err := <-pushed:
		t.Fatalf("Push did not block on a full queue: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if _, err := q.Pop(ctx); err != nil {
		t.Fatalf("Pop: %v", err)
	}
	select {
	case err := <-pushed:
		if err != nil {
			t.Fatalf("Push after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Push stayed blocked after the queue was drained")
	}

	// Cancellation unblocks a waiting Push.
	blockedCtx, cancel := context.WithCancel(ctx)
	go func() { pushed <- q.Push(blockedCtx, streamFrame{data: make([]byte, maxBytes*3)}) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-pushed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Push after cancel returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Push did not unblock on context cancellation")
	}

	// Once closed, Pop drains remaining frames and then reports EOF.
	q.Close()
	if _, err := q.Pop(ctx); err != nil {
		t.Fatalf("Pop after Close should drain: %v", err)
	}
	if _, err := q.Pop(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("Pop on drained closed queue = %v, want io.EOF", err)
	}
	if err := q.Push(ctx, streamFrame{data: []byte("x")}); !errors.Is(err, errStreamQueueClosed) {
		t.Fatalf("Push after Close = %v, want errStreamQueueClosed", err)
	}
}

// TestBoundedStreamQueuePopCanceled verifies Pop unblocks when its context is
// canceled while the queue is empty and open.
func TestBoundedStreamQueuePopCanceled(t *testing.T) {
	q := newBoundedStreamQueue(10)
	ctx, cancel := context.WithCancel(context.Background())

	popped := make(chan error, 1)
	go func() {
		_, err := q.Pop(ctx)
		popped <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-popped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Pop after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Pop did not unblock on context cancellation")
	}
}

// TestForwardResponseStreamSlowClientBackpressure drives the full forwarding
// path with a writer that never drains and asserts the pull goroutine stops
// once the buffered byte limit is reached.
func TestForwardResponseStreamSlowClientBackpressure(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	ctx := NewServerMetadataContext(parent, ServerMetadata{})
	req := httptest.NewRequest("GET", "http://example.com/foo", nil).WithContext(parent)
	w := newBlockableWriter(parent)

	frameSize := 1024
	marshaler := &fixedSizeMarshaler{size: frameSize}
	recv, recvCalls := countingRecv(t, 100, false)

	done := make(chan struct{})
	go func() {
		ForwardResponseStream(ctx, NewServeMux(), marshaler, w, req, recv)
		close(done)
	}()

	// Each queued frame occupies frameSize + 1 bytes (payload plus delimiter).
	// Allow slack for the frame currently in flight, then assert the pull count
	// stays bounded while the client never reads.
	wantMaxRecv := defaultMaxStreamBufferBytes/(frameSize+1) + 2
	waitFor(t, "queue to fill", func() bool { return recvCalls.Load() >= 2 })
	time.Sleep(50 * time.Millisecond)
	if got := int(recvCalls.Load()); got > wantMaxRecv {
		t.Fatalf("recv called %d times while client is stalled, want at most %d (buffer unbounded?)", got, wantMaxRecv)
	}
	before := recvCalls.Load()
	time.Sleep(50 * time.Millisecond)
	if recvCalls.Load() != before {
		t.Fatalf("recv count grew from %d to %d while the client never read", before, recvCalls.Load())
	}

	// Releasing the client lets buffered frames drain and pulling resumes.
	w.unblock()
	cancelParent()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ForwardResponseStream did not return after the client caught up")
	}
	if w.written.Load() == 0 {
		t.Fatal("no data reached the client after it caught up")
	}
}

// TestForwardResponseStreamClientDisconnect cancels the parent (request)
// context as a client disconnect would and verifies pulling stops and the
// handler returns promptly without further writes.
func TestForwardResponseStreamClientDisconnect(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx := NewServerMetadataContext(parent, ServerMetadata{})
	req := httptest.NewRequest("GET", "http://example.com/foo", nil).WithContext(parent)
	w := newBlockableWriter(parent)

	recv, recvCalls := countingRecv(t, 0, false)

	done := make(chan struct{})
	go func() {
		ForwardResponseStream(ctx, NewServeMux(), &JSONPb{}, w, req, recv)
		close(done)
	}()

	waitFor(t, "first frame to be pulled", func() bool { return recvCalls.Load() > 0 })
	cancelParent()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ForwardResponseStream did not return after the client disconnected")
	}

	callsAfter := recvCalls.Load()
	time.Sleep(50 * time.Millisecond)
	if recvCalls.Load() != callsAfter {
		t.Fatalf("recv kept being called after disconnect: %d -> %d", callsAfter, recvCalls.Load())
	}
}

// TestForwardResponseStreamWriteFailure makes every write fail and asserts the
// upstream pull is canceled and no further frame is written after the failure.
func TestForwardResponseStreamWriteFailure(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	ctx := NewServerMetadataContext(parent, ServerMetadata{})
	req := httptest.NewRequest("GET", "http://example.com/foo", nil).WithContext(parent)
	w := newBlockableWriter(parent)
	w.writeErr = errors.New("connection reset by peer")
	w.unblock()

	recv, _ := countingRecv(t, 0, false)

	done := make(chan struct{})
	go func() {
		ForwardResponseStream(ctx, NewServeMux(), &JSONPb{}, w, req, recv)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ForwardResponseStream did not return after a write failure")
	}
	if got := w.writeCalls.Load(); got != 1 {
		t.Fatalf("Write called %d times after a failure, want exactly 1 (no further frames)", got)
	}
}

// flushErrorWriter accepts writes but every flush fails with a generic error
// (not http.ErrNotSupported), via the FlushError interface that
// http.ResponseController.Flush actually honors.
type flushErrorWriter struct {
	header     http.Header
	writeCalls atomic.Int64
}

func (f *flushErrorWriter) Header() http.Header {
	if f.header == nil {
		f.header = http.Header{}
	}
	return f.header
}
func (f *flushErrorWriter) Write(p []byte) (int, error) {
	f.writeCalls.Add(1)
	return len(p), nil
}
func (f *flushErrorWriter) WriteHeader(int)   {}
func (f *flushErrorWriter) FlushError() error { return errors.New("flush failed: broken pipe") }

// TestForwardResponseStreamFlushFailure ensures a failed flush stops all
// further frame writes instead of writing the next frame anyway.
func TestForwardResponseStreamFlushFailure(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	ctx := NewServerMetadataContext(parent, ServerMetadata{})
	req := httptest.NewRequest("GET", "http://example.com/foo", nil).WithContext(parent)
	w := &flushErrorWriter{}

	recv, _ := countingRecv(t, 0, false)

	done := make(chan struct{})
	go func() {
		ForwardResponseStream(ctx, NewServeMux(), &JSONPb{}, w, req, recv)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ForwardResponseStream did not return after a flush failure")
	}
	if got := w.writeCalls.Load(); got != 1 {
		t.Fatalf("Write called %d times after flush failure, want exactly 1", got)
	}
}

// recordingWriter captures everything written to a streaming response. Unlike
// httptest.ResponseRecorder it implements FlushError, so it exercises the same
// ResponseController.Flush path a real net/http server takes, and it retains
// Trailer: prefixed headers verbatim instead of materializing trailers on
// Result like the real server would.
type recordingWriter struct {
	header http.Header
	body   bytes.Buffer
}

func (r *recordingWriter) Header() http.Header {
	if r.header == nil {
		r.header = http.Header{}
	}
	return r.header
}
func (r *recordingWriter) Write(p []byte) (int, error) { return r.body.Write(p) }
func (r *recordingWriter) WriteHeader(int)             {}
func (r *recordingWriter) FlushError() error           { return nil }

// TestForwardResponseStreamEOF flushes buffered frames on a normal upstream EOF
// and emits the gRPC trailers as HTTP trailers.
func TestForwardResponseStreamEOF(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	md := ServerMetadata{TrailerMD: metadata.Pairs("foo", "bar", "baz", "qux")}
	ctx := NewServerMetadataContext(parent, md)
	req := httptest.NewRequest("GET", "http://example.com/foo", nil).WithContext(parent)
	req.Header.Set("Te", "trailers")
	resp := &recordingWriter{}

	msgs := []proto.Message{
		&pb.SimpleMessage{Id: "One"},
		&pb.SimpleMessage{Id: "Two"},
	}
	var idx atomic.Int64
	recv := func() (proto.Message, error) {
		i := int(idx.Add(1)) - 1
		if i >= len(msgs) {
			return nil, io.EOF
		}
		return msgs[i], nil
	}

	ForwardResponseStream(ctx, NewServeMux(), &JSONPb{}, resp, req, recv)

	body := resp.body.String()
	if !strings.Contains(body, "One") || !strings.Contains(body, "Two") {
		t.Fatalf("buffered frames were not flushed on EOF: %q", body)
	}
	trailers := http.Header{}
	for k, vs := range resp.header {
		if strings.HasPrefix(k, http.TrailerPrefix) {
			trailers[k[len(http.TrailerPrefix):]] = vs
		}
	}
	if got := trailers.Get("Grpc-Trailer-Foo"); got != "bar" {
		t.Fatalf("trailer Grpc-Trailer-Foo = %q, want bar; trailers=%v", got, trailers)
	}
	if got := trailers.Get("Grpc-Trailer-Baz"); got != "qux" {
		t.Fatalf("trailer Grpc-Trailer-Baz = %q, want qux; trailers=%v", got, trailers)
	}
}

// TestForwardResponseStreamNoTrailersWithoutTE checks trailers are not emitted
// unless the request advertises TE: trailers, matching unary forwarding.
func TestForwardResponseStreamNoTrailersWithoutTE(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	md := ServerMetadata{TrailerMD: metadata.Pairs("foo", "bar")}
	ctx := NewServerMetadataContext(parent, md)
	req := httptest.NewRequest("GET", "http://example.com/foo", nil).WithContext(parent)
	resp := &recordingWriter{}

	var idx atomic.Int64
	recv := func() (proto.Message, error) {
		if idx.Add(1) > 1 {
			return nil, io.EOF
		}
		return &pb.SimpleMessage{Id: "One"}, nil
	}

	ForwardResponseStream(ctx, NewServeMux(), &JSONPb{}, resp, req, recv)

	for k := range resp.header {
		if strings.HasPrefix(k, http.TrailerPrefix) || k == "Trailer" {
			t.Fatalf("unexpected trailer header %q without TE: trailers", k)
		}
	}
}

// TestForwardResponseStreamUpstreamErrorDeliveredOnce sends messages followed
// by an upstream error while the client is stalled, then releases the client.
// The error chunk must reach the client exactly once and pulling must stop at
// the error.
func TestForwardResponseStreamUpstreamErrorDeliveredOnce(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	ctx := NewServerMetadataContext(parent, ServerMetadata{})
	req := httptest.NewRequest("GET", "http://example.com/foo", nil).WithContext(parent)
	w := newBlockableWriter(parent)

	streamErr := status.Errorf(codes.OutOfRange, "400")
	const okMsgs = 20
	var recvCalls atomic.Int64
	recv := func() (proto.Message, error) {
		n := recvCalls.Add(1)
		if int(n) <= okMsgs {
			return &pb.SimpleMessage{Id: "x"}, nil
		}
		return nil, streamErr
	}

	done := make(chan struct{})
	go func() {
		ForwardResponseStream(ctx, NewServeMux(), &JSONPb{}, w, req, recv)
		close(done)
	}()

	waitFor(t, "error to be pulled", func() bool { return recvCalls.Load() >= okMsgs+1 })
	w.unblock()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ForwardResponseStream did not return after the upstream error")
	}
	if got := recvCalls.Load(); got != okMsgs+1 {
		t.Fatalf("recv called %d times, want %d (pulling did not stop on error)", got, okMsgs+1)
	}
	if got := w.writeCalls.Load(); got < 2 {
		t.Fatalf("Write called %d times, want data frames plus one error frame", got)
	}
	// The final write is the error chunk + delimiter; it must contain exactly
	// one serialized status.
	if n := strings.Count(string(w.snapshot()), `"error"`); n != 1 {
		t.Fatalf("error chunk delivered %d times, want exactly 1", n)
	}
}

// TestForwardResponseStreamReleasesGoroutines verifies the producer goroutine
// and any synchronization machinery are gone when the response ends, including
// after a client disconnect while pulling is blocked on the bounded queue.
func TestForwardResponseStreamReleasesGoroutines(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx := NewServerMetadataContext(parent, ServerMetadata{})
	req := httptest.NewRequest("GET", "http://example.com/foo", nil).WithContext(parent)
	w := newBlockableWriter(parent)

	recv, _ := countingRecv(t, 0, false)

	done := make(chan struct{})
	go func() {
		ForwardResponseStream(ctx, NewServeMux(), &JSONPb{}, w, req, recv)
		close(done)
	}()

	waitFor(t, "stream to start", func() bool { return w.writeCalls.Load() > 0 })
	cancelParent()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ForwardResponseStream did not return on disconnect")
	}

	// No goroutine owned by the forwarder should remain parked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leaked := false
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		for _, line := range strings.Split(string(buf[:n]), "\n") {
			if strings.Contains(line, "streamForwardProducer") || strings.Contains(line, "boundedStreamQueue.Push") {
				leaked = true
			}
		}
		if !leaked {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("stream forwarding goroutines leaked after the response ended")
}
