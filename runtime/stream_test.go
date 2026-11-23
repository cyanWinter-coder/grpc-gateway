package runtime_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/grpc-ecosystem/grpc-gateway/v2/runtime/internal/examplepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// disconnectWriter fails every write and flush after disconnect is closed,
// modeling a broken client connection whose socket no longer accepts data.
type disconnectWriter struct {
	http.ResponseWriter
	disconnect <-chan struct{}
	writeCalls atomic.Int64
}

func (d *disconnectWriter) Write(p []byte) (int, error) {
	d.writeCalls.Add(1)
	select {
	case <-d.disconnect:
		return 0, errClientDisconnected
	default:
	}
	return d.ResponseWriter.Write(p)
}

func (d *disconnectWriter) Flush() {
	select {
	case <-d.disconnect:
		return
	default:
	}
	if f, ok := d.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

var errClientDisconnected = errors.New("client disconnected: broken pipe")

// TestForwardResponseStreamEndToEndClientDisconnect simulates a client that
// hangs up mid-stream and verifies the upstream pull loop stops and the
// handler returns rather than draining the whole upstream stream.
func TestForwardResponseStreamEndToEndClientDisconnect(t *testing.T) {
	disconnect := make(chan struct{})
	var recvCalls atomic.Int64
	firstFrame := make(chan struct{})
	handlerReturned := make(chan struct{})

	mux := runtime.NewServeMux()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerReturned)
		ctx := runtime.NewServerMetadataContext(r.Context(), runtime.ServerMetadata{})
		// A real gRPC ClientStream.Recv unblocks with an error when the RPC
		// context is canceled; mirror that by watching the request context.
		recv := func() (proto.Message, error) {
			if err := r.Context().Err(); err != nil {
				return nil, err
			}
			n := recvCalls.Add(1)
			if n == 1 {
				close(firstFrame)
			}
			return &pb.SimpleMessage{Id: "x"}, nil
		}
		runtime.ForwardResponseStream(ctx, mux, &runtime.JSONPb{}, &disconnectWriter{
			ResponseWriter: w,
			disconnect:     disconnect,
		}, r, recv)
	}))
	defer server.Close()

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}

	// Wait until the first frame has been pulled, then hang up: the writer
	// starts failing and the stream must wind down without draining upstream.
	<-firstFrame
	_ = resp.Body.Close()
	close(disconnect)

	select {
	case <-handlerReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after the client disconnected")
	}

	calls := recvCalls.Load()
	time.Sleep(100 * time.Millisecond)
	if recvCalls.Load() != calls {
		t.Fatalf("recv kept running after disconnect: %d -> %d", calls, recvCalls.Load())
	}
}

// TestForwardResponseStreamEndToEndEOFAndTrailers verifies a normal upstream
// EOF flushes every frame and delivers gRPC trailers as HTTP trailers through
// a real HTTP/1.1 chunked response.
func TestForwardResponseStreamEndToEndEOFAndTrailers(t *testing.T) {
	mux := runtime.NewServeMux()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := runtime.NewServerMetadataContext(r.Context(), runtime.ServerMetadata{
			TrailerMD: metadata.Pairs("foo", "bar"),
		})
		msgs := []proto.Message{
			&pb.SimpleMessage{Id: "One"},
			&pb.SimpleMessage{Id: "Two"},
		}
		var idx int
		recv := func() (proto.Message, error) {
			if idx >= len(msgs) {
				return nil, io.EOF
			}
			msg := msgs[idx]
			idx++
			return msg, nil
		}
		runtime.ForwardResponseStream(ctx, mux, &runtime.JSONPb{}, w, r, recv)
	}))
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Te", "trailers")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), "One") || !strings.Contains(string(body), "Two") {
		t.Fatalf("not all frames flushed on EOF: %q", body)
	}
	if got := resp.Trailer.Get("Grpc-Trailer-Foo"); got != "bar" {
		t.Fatalf("HTTP trailer Grpc-Trailer-Foo = %q, want bar; trailers=%v", got, resp.Trailer)
	}
}
