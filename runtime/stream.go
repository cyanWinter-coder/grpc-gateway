package runtime

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/textproto"
	"sync"

	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// defaultMaxStreamBufferBytes bounds the marshaled output that may be buffered
// waiting for a slow client before the goroutine pulling from the upstream gRPC
// stream stops reading more messages.
const defaultMaxStreamBufferBytes = 32 << 10 // 32 KiB

// errStreamQueueClosed is returned by boundedStreamQueue operations once the
// queue has been closed. It only crosses goroutine boundaries inside this file.
var errStreamQueueClosed = errors.New("stream frame queue closed")

// streamFrame is one marshaled upstream message together with the metadata
// needed to write it to the HTTP response.
type streamFrame struct {
	// resp is the unmarshaled message, retained so forward response options can
	// still inspect it on the writer goroutine.
	resp proto.Message
	// data holds the marshaled message followed by the stream delimiter.
	data []byte
	// contentType is set on the first frame and copied onto the response header
	// before the first write.
	contentType string
}

// boundedStreamQueue is a FIFO queue of stream frames whose buffered payload is
// capped at maxBytes. Push blocks while the limit is exceeded, so a slow HTTP
// client applies backpressure all the way to the upstream Recv loop.
type boundedStreamQueue struct {
	// frames[head:] are the queued frames. The head index avoids shifting the
	// slice on every Pop; the slice is reset when it drains so popped frame
	// buffers are released instead of being retained by the backing array.
	frames   []streamFrame
	head     int
	buffered int
	maxBytes int

	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
}

func newBoundedStreamQueue(maxBytes int) *boundedStreamQueue {
	q := &boundedStreamQueue{maxBytes: maxBytes}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Push appends a frame, waiting while another frame is already buffered beyond
// the byte limit or while ctx is canceled. A frame larger than the limit is
// accepted on its own so an oversized message cannot deadlock the stream.
func (q *boundedStreamQueue) Push(ctx context.Context, frame streamFrame) error {
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()

	for !q.closed && q.buffered > 0 && q.buffered+len(frame.data) > q.maxBytes {
		q.cond.Wait()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if q.closed {
		return errStreamQueueClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if q.head == len(q.frames) {
		// The queue drained since the last reset; start fresh so the backing
		// array from the previous burst can be reclaimed.
		q.frames = q.frames[:0]
		q.head = 0
	}
	q.frames = append(q.frames, frame)
	q.buffered += len(frame.data)
	// A frame may be waiting in Pop for the queue to become non-empty.
	q.cond.Signal()
	return nil
}

// Pop removes and returns the next frame, waiting while the queue is empty. It
// returns io.EOF once the queue is closed and drained, or ctx.Err if ctx is
// canceled first.
func (q *boundedStreamQueue) Pop(ctx context.Context) (streamFrame, error) {
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()

	for q.head == len(q.frames) && !q.closed {
		q.cond.Wait()
		if err := ctx.Err(); err != nil {
			return streamFrame{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return streamFrame{}, err
	}
	if q.head == len(q.frames) {
		return streamFrame{}, io.EOF
	}

	frame := q.frames[q.head]
	q.frames[q.head] = streamFrame{}
	q.head++
	if q.head == len(q.frames) {
		// Fully drained: drop the backing array so all popped buffers are
		// reclaimed; the next Push resets and reallocates.
		q.frames = nil
		q.head = 0
	}
	q.buffered -= len(frame.data)
	// A producer may be waiting for room in Push.
	q.cond.Signal()
	return frame, nil
}

// Close marks the queue as closed. Frames already queued remain available via
// Pop until drained; further Pushes fail. Close is safe to call repeatedly.
func (q *boundedStreamQueue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

// streamResult carries the terminal outcome of the upstream pull goroutine.
type streamResult struct {
	err error
}

// ForwardResponseStream forwards the stream from gRPC server to REST client.
//
// Upstream messages are pulled on a separate goroutine and handed to the
// writer through a byte-bounded queue: when a slow client cannot keep up, the
// buffered frames stop growing and the pull goroutine blocks instead of
// reading further upstream. A client disconnect or any write failure cancels
// the stream context so the upstream call is released promptly.
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

	// streamCtx is owned by this handler: it is canceled on client disconnect
	// or writer failure and always released when the response finishes.
	streamCtx, cancel := context.WithCancel(ctx)

	queue := newBoundedStreamQueue(defaultMaxStreamBufferBytes)
	// Buffered so the producer goroutine can always deliver its terminal result
	// without waiting for the writer goroutine, which may have failed first.
	resultCh := make(chan streamResult, 1)
	producerDone := make(chan struct{})

	go func() {
		defer close(producerDone)
		// Closing the queue lets the writer drain buffered frames and observe
		// EOF even when the producer exits early.
		defer queue.Close()
		streamForwardProducer(streamCtx, mux, marshaler, delimiter, recv, queue, resultCh)
	}()

	var wroteHeader bool
	var finalErr error
writer:
	for {
		frame, err := queue.Pop(streamCtx)
		switch {
		case errors.Is(err, io.EOF):
			// The producer closed the queue after it stopped pulling. Its
			// terminal result, when present, decides whether this is a clean
			// EOF or an error. A missing result means it exited because the
			// stream context was canceled, in which case no error is delivered.
			select {
			case res := <-resultCh:
				if res.err != nil {
					finalErr = res.err
					break writer
				}
			default:
			}
			// Upstream reached a normal end: flush whatever remains and forward
			// the gRPC trailers.
			if err := rc.Flush(); err != nil {
				if errors.Is(err, http.ErrNotSupported) {
					grpclog.Errorf("Flush not supported in %T", w)
					http.Error(w, "unexpected type of web server", http.StatusInternalServerError)
					break writer
				}
				grpclog.Errorf("Failed to flush response to client: %v", err)
				break writer
			}
			if requestAcceptsTrailers(req) {
				handleForwardResponseStreamTrailer(w, mux, md)
			}
			break writer
		case err != nil:
			// streamCtx cancellation (client disconnect or a previous write
			// failure): stop pulling and do not attempt any further writes.
			break writer
		}

		if err := handleForwardResponseOptions(ctx, w, frame.resp, opts); err != nil {
			finalErr = err
			break writer
		}
		if !wroteHeader {
			w.Header().Set("Content-Type", frame.contentType)
		}
		if _, err := w.Write(frame.data); err != nil {
			grpclog.Errorf("Failed to send response chunk: %v", err)
			// The error is terminal and must not be delivered a second time;
			// the cleanup below cancels the upstream context.
			break writer
		}
		wroteHeader = true
		if err := rc.Flush(); err != nil {
			if errors.Is(err, http.ErrNotSupported) {
				grpclog.Errorf("Flush not supported in %T", w)
				http.Error(w, "unexpected type of web server", http.StatusInternalServerError)
				break writer
			}
			grpclog.Errorf("Failed to flush response to client: %v", err)
			// A failed flush means the connection is gone: never write another
			// frame. Cleanup below cancels the upstream context.
			break writer
		}
	}

	// The response is over: cancel the stream context and close the queue so
	// both queue operations wake immediately, then wait for the pull goroutine
	// to exit. Closing the queue is what unblocks a producer parked in Push:
	// context cancellation alone is not observed until it reacquires the
	// condition variable. The wait matters when recv() is blocked, since the
	// context shared with the upstream gRPC stream is only canceled once this
	// handler returns, and returning early would leak the pull goroutine.
	cancel()
	queue.Close()
	<-producerDone

	if finalErr != nil {
		// The single terminal error delivery path; no further frames are
		// written after this point.
		handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, finalErr, delimiter)
	}
}

// streamForwardProducer pulls messages from the upstream stream, marshals them
// and pushes one frame at a time into queue. It blocks inside Push when the
// queue is full, which stops further Recv calls until the client catches up.
func streamForwardProducer(ctx context.Context, mux *ServeMux, marshaler Marshaler, delimiter []byte, recv func() (proto.Message, error), queue *boundedStreamQueue, resultCh chan<- streamResult) {
	firstFrame := true
	for {
		if err := ctx.Err(); err != nil {
			// The writer gave up on this response; exit without touching the
			// result channel so the error is delivered only once.
			return
		}

		resp, err := recv()
		if errors.Is(err, io.EOF) {
			resultCh <- streamResult{}
			return
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			resultCh <- streamResult{err: err}
			return
		}

		respRw, err := mux.forwardResponseRewriter(ctx, resp)
		if err != nil {
			grpclog.Errorf("Rewrite error: %v", err)
			if ctx.Err() != nil {
				return
			}
			resultCh <- streamResult{err: err}
			return
		}

		var contentType string
		if firstFrame {
			if sct, ok := marshaler.(StreamContentType); ok {
				contentType = sct.StreamContentType(respRw)
			} else {
				contentType = marshaler.ContentType(respRw)
			}
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
			if ctx.Err() != nil {
				return
			}
			resultCh <- streamResult{err: err}
			return
		}

		// One allocation carries the payload and its delimiter so the writer
		// performs a single Write per frame.
		data := make([]byte, 0, len(buf)+len(delimiter))
		data = append(data, buf...)
		data = append(data, delimiter...)

		frame := streamFrame{
			resp:        resp,
			data:        data,
			contentType: contentType,
		}
		if err := queue.Push(ctx, frame); err != nil {
			// Writer-side cancellation or a closed queue; nothing to report.
			return
		}
		firstFrame = false
	}
}

// handleForwardResponseStreamTrailer writes the gRPC trailer metadata as HTTP
// trailers after the response body. Trailer keys are prefixed with
// http.TrailerPrefix so they are emitted even though the trailer names were not
// known before the first write.
func handleForwardResponseStreamTrailer(w http.ResponseWriter, mux *ServeMux, md ServerMetadata) {
	for k, vs := range md.TrailerMD {
		h, ok := mux.outgoingTrailerMatcher(k)
		if !ok {
			continue
		}
		// gRPC metadata keys are lowercase; HTTP trailer keys must be canonical.
		h = textproto.CanonicalMIMEHeaderKey(h)
		for _, v := range vs {
			w.Header().Add(http.TrailerPrefix+h, v)
		}
	}
}
