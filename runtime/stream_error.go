package runtime

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Canonical HTTP trailer names used for the terminal status of a response
// stream. They follow the gRPC over HTTP trailer convention.
const (
	grpcStatusTrailerName           = "Grpc-Status"
	grpcMessageTrailerName          = "Grpc-Message"
	grpcStatusDetailsBinTrailerName = "Grpc-Status-Details-Bin"

	// StreamStatusTrailer is the terminal gRPC status code trailer.
	StreamStatusTrailer = grpcStatusTrailerName
	// StreamMessageTrailer is the terminal percent-encoded gRPC message trailer.
	StreamMessageTrailer = grpcMessageTrailerName
	// StreamStatusDetailsBinTrailer is the base64 google.rpc.Status trailer.
	StreamStatusDetailsBinTrailer = grpcStatusDetailsBinTrailerName
)

// Transport terminal kinds. They are intentionally private: callers observe
// the mapped HTTP/gRPC statuses, while this package uses the kind to refine the
// HTTP status without letting representations drift.
type streamTransportKind int

const (
	streamTransportNone streamTransportKind = iota
	streamTransportClientCanceled
	streamTransportDeadline
	streamTransportUpstreamDisconnected
)

// Trailer values are header fields. Keep the complete terminal trailer block
// comfortably below Go net/http's 4 KiB read buffer and common proxy header
// limits; budgeting each field independently could otherwise let an oversized
// details trailer evict the terminal grpc-status field.
const (
	maxTrailerMessageBytes   = 1 << 10
	maxTrailerDetailsEncoded = 2 << 10
	trailerTruncationSuffix  = "..."
)

var streamStatusTrailerNames = []string{
	grpcStatusTrailerName,
	grpcMessageTrailerName,
	grpcStatusDetailsBinTrailerName,
}

// HTTPStatusFromError maps an error to the HTTP status used for a terminal
// gateway response. In addition to the gRPC code mapping performed by
// HTTPStatusFromCode, it distinguishes a client cancellation from an upstream
// transport disconnect and honors an explicit HTTPStatusError.
//
// Non-streaming error handlers retain their established behavior; this helper
// is intended for stream terminal states and callers that want the transport
// distinction.
func HTTPStatusFromError(err error) int {
	_, httpStatus := resolveStreamTerminal(context.Background(), nil, err, status.Convert(err))
	return httpStatus
}

// resolveStreamTerminal returns the stable gRPC status used in the body and
// trailers together with the HTTP terminal status. The configured stream error
// handler is authoritative for explicit gRPC statuses; classification only
// fills in raw context and network errors, whose default conversion is
// codes.Unknown.
func resolveStreamTerminal(ctx context.Context, req *http.Request, err error, st *status.Status) (*status.Status, int) {
	if st == nil {
		st = status.Convert(err)
	}
	kind := classifyStreamTransport(ctx, req, err)
	switch st.Code() {
	case codes.Unknown:
		switch kind {
		case streamTransportClientCanceled:
			st = statusFromProtoWithCode(st, codes.Canceled)
		case streamTransportDeadline:
			st = statusFromProtoWithCode(st, codes.DeadlineExceeded)
		case streamTransportUpstreamDisconnected:
			st = statusFromProtoWithCode(st, codes.Unavailable)
		}
	case codes.Internal, codes.DataLoss:
		// A read failure can be reported as Internal/DataLoss with the transport
		// cause only in its message. When the cause is unmistakably a lost
		// upstream connection, normalize both representations to Unavailable.
		if kind == streamTransportUpstreamDisconnected {
			st = statusFromProtoWithCode(st, codes.Unavailable)
		}
	}
	return st, streamTerminalHTTPStatus(err, st, kind)
}

func statusFromProtoWithCode(st *status.Status, code codes.Code) *status.Status {
	sp := st.Proto()
	if sp == nil {
		return status.New(code, st.Message())
	}
	sp.Code = int32(code)
	return status.FromProto(sp)
}

func streamTerminalHTTPStatus(err error, st *status.Status, kind streamTransportKind) int {
	var customStatus *HTTPStatusError
	if errors.As(err, &customStatus) {
		return customStatus.HTTPStatus
	}

	// Only refine the canonical mapping when the gRPC status agrees with the
	// transport classification. This keeps Grpc-Status and the early HTTP
	// status representing the same terminal outcome.
	if st.Code() == codes.Unavailable && kind == streamTransportUpstreamDisconnected {
		// A connection-level upstream loss is a bad gateway rather than a
		// normally unavailable service (which remains 503).
		return http.StatusBadGateway
	}
	return HTTPStatusFromCode(st.Code())
}

func classifyStreamTransport(ctx context.Context, req *http.Request, err error) streamTransportKind {
	if clientCancelled(ctx, req, err) {
		return streamTransportClientCanceled
	}
	if clientDeadlineExceeded(ctx, req, err) {
		return streamTransportDeadline
	}
	if upstreamDisconnected(err) {
		return streamTransportUpstreamDisconnected
	}
	return streamTransportNone
}

func clientCancelled(ctx context.Context, req *http.Request, err error) bool {
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return true
	}
	for _, candidate := range []context.Context{ctx, requestContext(req)} {
		if candidate != nil && errors.Is(candidate.Err(), context.Canceled) {
			return true
		}
	}
	return false
}

func clientDeadlineExceeded(ctx context.Context, req *http.Request, err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return true
	}
	for _, candidate := range []context.Context{ctx, requestContext(req)} {
		if candidate != nil && errors.Is(candidate.Err(), context.DeadlineExceeded) {
			return true
		}
	}
	return false
}

func requestContext(req *http.Request) context.Context {
	if req == nil {
		return nil
	}
	return req.Context()
}

func upstreamDisconnected(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) {
		return true
	}

	// Textual transport matching is only safe for statuses that already denote
	// an internal/transport failure. An InvalidArgument whose description
	// happens to contain "disconnect", for example, must keep its 400 mapping.
	switch status.Code(err) {
	case codes.Unknown, codes.Internal, codes.Unavailable, codes.DataLoss:
	default:
		return false
	}

	// gRPC usually converts transport failures to codes.Unavailable, but the
	// original network error is only represented by its text in the status
	// message. Match the common transport forms without changing the canonical
	// Unavailable -> 503 mapping for an ordinary unavailable service.
	msg := strings.ToLower(err.Error())
	for _, fragment := range []string{
		"connection aborted",
		"connection closed",
		"connection error",
		"connection reset",
		"connection refused",
		"disconnect",
		"broken pipe",
		"error reading from server",
		"error writing to server",
		"server closed the connection",
		"transport is closing",
		"unexpected eof",
	} {
		if strings.Contains(msg, fragment) {
			return true
		}
	}
	return false
}

// announceStreamStatusTrailers declares the terminal stream trailers before
// the response status/body is committed. It reports whether the names were
// announced using the standard predeclared-trailer mechanism. When false,
// writeStreamStatusTrailers uses http.TrailerPrefix, which still records and
// sends values as trailers without requiring a Trailer announcement.
func announceStreamStatusTrailers(w http.ResponseWriter, req *http.Request, mux *ServeMux) bool {
	if !streamTrailersAnnouncementEnabled(req, mux) {
		return false
	}
	for _, name := range streamStatusTrailerNames {
		announceTrailerName(w, name)
	}
	return true
}

// announceTrailerName declares name in the Trailer header exactly once.
func announceTrailerName(w http.ResponseWriter, name string) {
	for _, value := range w.Header().Values("Trailer") {
		for _, announced := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(announced), name) {
				return
			}
		}
	}
	w.Header().Add("Trailer", name)
}

func streamTrailersAnnouncementEnabled(req *http.Request, mux *ServeMux) bool {
	if mux != nil && mux.disableChunkedEncoding {
		return false
	}
	return req != nil && requestAcceptsTrailers(req)
}

// writeStreamTransportErrorTrailer records the terminal trailer after a local
// write or flush failure. Such a failure is on the HTTP/client side, not an
// upstream Recv failure. The HTTP status is already committed, so this must not
// call WriteHeader again.
func writeStreamTransportErrorTrailer(ctx context.Context, w http.ResponseWriter, req *http.Request, mux *ServeMux, err error) {
	st := status.Convert(err)
	switch {
	case errors.Is(err, http.ErrNotSupported):
		st = status.New(codes.Internal, "streaming flush is not supported")
	case clientDeadlineExceeded(ctx, req, err):
		st = status.New(codes.DeadlineExceeded, "client deadline exceeded")
	default:
		// A failed ResponseWriter.Write/Flush normally means the client went
		// away. Normalize unknown or transport-ish errors to Canceled; retain
		// an explicit application status produced by a response option.
		resolved, _ := resolveStreamTerminal(ctx, req, err, st)
		if code := resolved.Code(); code == codes.Unknown || code == codes.Unavailable {
			resolved = status.New(codes.Canceled, "client disconnected before the stream completed")
		}
		st = resolved
	}
	writeStreamStatusTrailers(w, streamTrailersAnnouncementEnabled(req, mux), st)
}

func writeStreamStatusTrailers(w http.ResponseWriter, announced bool, st *status.Status) {
	if st == nil {
		return
	}

	// Always use the TrailerPrefix mechanism for the values. It remains valid
	// if a forward response option committed the header before our Trailer
	// announcement was installed, and avoids bare declared keys accidentally
	// being emitted as regular headers on empty responses.
	_ = announced
	values := []struct {
		name  string
		value string
	}{
		{grpcStatusTrailerName, strconv.Itoa(int(st.Code()))},
		{grpcMessageTrailerName, encodeStreamStatusMessage(st.Message())},
		{grpcStatusDetailsBinTrailerName, truncatedStatusDetailsBin(st)},
	}
	for _, v := range values {
		if v.value == "" {
			continue
		}
		w.Header().Set(http.TrailerPrefix+v.name, v.value)
	}
}

// encodeStreamStatusMessage percent-encodes a gRPC message trailer using the
// same byte rules as gRPC-Go. Printable ASCII (including spaces) is preserved;
// controls, '%', DEL and non-ASCII bytes are encoded. The result contains no
// CR/LF and is truncated to the trailer budget without splitting a percent
// escape.
func encodeStreamStatusMessage(msg string) string {
	var b strings.Builder
	b.Grow(len(msg))

	var encodedRune [utf8.UTFMax]byte
	const hexDigits = "0123456789ABCDEF"
	for _, r := range msg {
		n := utf8.EncodeRune(encodedRune[:], r)
		for i := 0; i < n; i++ {
			c := encodedRune[i]
			if c >= 0x20 && c <= 0x7e && c != '%' {
				b.WriteByte(c)
				continue
			}
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0x0f])
		}
	}
	return truncateEncodedTrailerValue(b.String(), maxTrailerMessageBytes)
}

func truncateEncodedTrailerValue(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	if maxBytes <= len(trailerTruncationSuffix) {
		return strings.Repeat(".", maxBytes)
	}

	cut := maxBytes - len(trailerTruncationSuffix)
	// Do not leave a partial %XX escape at the truncation boundary.
	for cut > 0 && (value[cut-1] == '%' || (cut > 1 && value[cut-2] == '%')) {
		cut--
	}
	return value[:cut] + trailerTruncationSuffix
}

// truncateTrailerValue truncates at a byte boundary while keeping the result
// valid UTF-8 and no longer than maxBytes. The "..." suffix makes truncation
// visible to operators without introducing CR/LF that could alter header
// framing.
func truncateTrailerValue(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	if maxBytes <= len(trailerTruncationSuffix) {
		return strings.Repeat(".", maxBytes)
	}

	limit := maxBytes - len(trailerTruncationSuffix)
	// Move back to the first byte of the final complete rune.
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit] + trailerTruncationSuffix
}

// truncatedStatusDetailsBin returns the standard base64-encoded
// google.rpc.Status proto used by gRPC's grpc-status-details-bin trailer.
//
// Trailer blocks are much more size-constrained than the response body. Keep
// the top-level Status proto valid and preserve the first details/type URL by
// truncating an oversized detail value to fit. The complete details remain in
// the streamed error chunk when that chunk could be delivered.
func truncatedStatusDetailsBin(st *status.Status) string {
	if st == nil {
		return ""
	}
	sp := st.Proto()
	if sp == nil || len(sp.Details) == 0 {
		return ""
	}

	// base64 expands every 3 raw bytes to 4 encoded bytes. Round the raw budget
	// down to a multiple of 3 so the encoded value stays within
	// maxTrailerDetailsEncoded without padding-related overflow.
	const maxRaw = (maxTrailerDetailsEncoded / 4) * 3
	// The same message is already sent in Grpc-Message; keep only a short copy
	// here so the budget is available for details.
	sp.Message = truncateTrailerValue(sp.Message, 128)

	if proto.Size(sp) > maxRaw {
		allDetails := sp.Details
		sp.Details = nil
		for _, detail := range allDetails {
			sp.Details = append(sp.Details, detail)
			if proto.Size(sp) <= maxRaw {
				continue
			}

			originalValue := detail.Value
			best := -1
			lo, hi := 0, len(originalValue)
			for lo <= hi {
				mid := lo + (hi-lo)/2
				detail.Value = originalValue[:mid]
				if proto.Size(sp) <= maxRaw {
					best = mid
					lo = mid + 1
				} else {
					hi = mid - 1
				}
			}
			if best < 0 {
				// Even an empty value does not fit (usually an oversized type
				// URL); drop this and every later detail.
				sp.Details = sp.Details[:len(sp.Details)-1]
			} else {
				detail.Value = originalValue[:best]
			}
			break
		}
	}

	raw, err := proto.Marshal(sp)
	if err != nil || len(raw) == 0 || len(raw) > maxRaw {
		return ""
	}
	return base64.StdEncoding.EncodeToString(raw)
}
