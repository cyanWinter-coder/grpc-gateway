package runtime

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/encoding/protojson"
)

// MIMEWildcard is the fallback MIME type used for requests which do not match
// a registered MIME type.
const MIMEWildcard = "*"

// builtinJSONMIME is the media type of the marshaler every ServeMux starts
// with. It is also the first fallback considered when a negotiated marshaler
// fails to encode a response.
const builtinJSONMIME = "application/json"

var (
	acceptHeader      = http.CanonicalHeaderKey("Accept")
	contentTypeHeader = http.CanonicalHeaderKey("Content-Type")

	defaultMarshaler = &HTTPBodyMarshaler{
		Marshaler: &JSONPb{
			MarshalOptions: protojson.MarshalOptions{
				EmitUnpopulated: true,
			},
			UnmarshalOptions: protojson.UnmarshalOptions{
				DiscardUnknown: true,
			},
		},
	}

	// emptyMarshalerSnapshot is used by a ServeMux that was not built with
	// NewServeMux, so that lookups stay total instead of panicking.
	emptyMarshalerSnapshot = newMarshalerSnapshot(nil)
)

// MarshalerForRequest returns the inbound/outbound marshalers for this request.
// It checks the registry on the ServeMux for the MIME type set by the Content-Type header.
// If it isn't set (or the request Content-Type is empty), checks for "*".
// If there are multiple Content-Type headers set, choose the first one that it can
// exactly match in the registry.
// Otherwise, it follows the above logic for "*"/InboundMarshaler/OutboundMarshaler.
//
// The marshalers are selected from one immutable snapshot of the registry. For
// a request that entered through ServeMux, that is the snapshot taken when the
// request entered, so a media type registered or removed while the request is
// in flight does not change its encoding.
func MarshalerForRequest(mux *ServeMux, r *http.Request) (inbound Marshaler, outbound Marshaler) {
	return marshalerSnapshotForRequest(mux, r).marshalersForRequest(r)
}

// marshalerSnapshotKey is the context key under which ServeMux records the
// registry snapshot a request entered with.
type marshalerSnapshotKey struct{}

func withMarshalerSnapshot(ctx context.Context, snapshot *marshalerSnapshot) context.Context {
	return context.WithValue(ctx, marshalerSnapshotKey{}, snapshot)
}

// marshalerSnapshotForRequest returns the snapshot the request entered the mux
// with, or the mux's current snapshot for requests that did not go through
// ServeMux (for example, a handler invoked directly in a test).
func marshalerSnapshotForRequest(mux *ServeMux, r *http.Request) *marshalerSnapshot {
	if snapshot, ok := r.Context().Value(marshalerSnapshotKey{}).(*marshalerSnapshot); ok {
		return snapshot
	}
	return mux.currentMarshalerSnapshot()
}

// marshalerSnapshot is an immutable mapping from case-sensitive MIME types to
// Marshalers. Additions and removals produce a new snapshot instead of
// mutating this one, so a request that captured an older snapshot keeps a
// consistent view of the registry for its whole lifetime.
type marshalerSnapshot struct {
	mimeMap map[string]Marshaler
	// hasExact reports whether any registration other than MIMEWildcard
	// exists. Without one, nothing in an Accept header can select a marshaler,
	// so the header is not parsed at all.
	hasExact bool
}

// makeMarshalerMIMERegistry returns a new registry of marshalers.
// It allows for a mapping of case-sensitive Content-Type MIME type string to runtime.Marshaler interfaces.
//
// For example, you could allow the client to specify the use of the runtime.JSONPb marshaler
// with an "application/jsonpb" Content-Type and the use of the runtime.JSONBuiltin marshaler
// with an "application/json" Content-Type.
// "*" can be used to match any Content-Type.
// This can be attached to a ServerMux with the marshaler option.
func makeMarshalerMIMERegistry() *marshalerSnapshot {
	return newMarshalerSnapshot(map[string]Marshaler{
		MIMEWildcard: defaultMarshaler,
	})
}

func newMarshalerSnapshot(mimeMap map[string]Marshaler) *marshalerSnapshot {
	cp := make(map[string]Marshaler, len(mimeMap))
	hasExact := false
	for mime, marshaler := range mimeMap {
		cp[mime] = marshaler
		if mime != MIMEWildcard {
			hasExact = true
		}
	}
	return &marshalerSnapshot{mimeMap: cp, hasExact: hasExact}
}

func (m *marshalerSnapshot) lookup(mime string) (Marshaler, bool) {
	if m == nil {
		return nil, false
	}
	marshaler, ok := m.mimeMap[mime]
	return marshaler, ok && marshaler != nil
}

// with returns a new snapshot that also maps mime to marshaler.
func (m *marshalerSnapshot) with(mime string, marshaler Marshaler) *marshalerSnapshot {
	next := newMarshalerSnapshot(m.mimeMap)
	next.mimeMap[mime] = marshaler
	return next
}

// without returns a new snapshot without mime, and whether it was present.
func (m *marshalerSnapshot) without(mime string) (*marshalerSnapshot, bool) {
	if _, ok := m.mimeMap[mime]; !ok {
		return m, false
	}
	next := newMarshalerSnapshot(m.mimeMap)
	delete(next.mimeMap, mime)
	return next, true
}

func (m *marshalerSnapshot) marshalersForRequest(r *http.Request) (inbound Marshaler, outbound Marshaler) {
	outbound = m.outboundForRequest(r)
	inbound = m.inboundForRequest(r)
	if inbound == nil {
		inbound = m.fallbackMarshaler()
	}
	if outbound == nil {
		outbound = inbound
	}
	return inbound, outbound
}

// inboundForRequest resolves the Content-Type header. Exact registry keys win,
// then the parsed media type, then MIMEWildcard, then the built-in JSON
// marshaler, so the fallback order does not depend on map iteration.
func (m *marshalerSnapshot) inboundForRequest(r *http.Request) Marshaler {
	for _, contentTypeVal := range r.Header[contentTypeHeader] {
		// A header that is already exactly a registered media type -- "application/json",
		// which is what clients send -- needs no parsing. mime.ParseMediaType allocates a
		// parameter map even when the value carries no parameters, and it would return this
		// value unchanged, so the lookups below would be the same one.
		if marshaler, ok := m.lookup(contentTypeVal); ok {
			return marshaler
		}
		contentType, _, err := mime.ParseMediaType(contentTypeVal)
		if err != nil {
			grpclog.Errorf("Failed to parse Content-Type %s: %v", contentTypeVal, err)
			continue
		}
		if marshaler, ok := m.lookup(contentType); ok {
			return marshaler
		}
	}
	return nil
}

// fallbackMarshaler is the marshaler for requests that matched nothing in the
// snapshot. MIMEWildcard is the documented fallback, but it can be removed at
// runtime, so a built-in JSON marshaler keeps the request encodable.
func (m *marshalerSnapshot) fallbackMarshaler() Marshaler {
	if marshaler, ok := m.lookup(MIMEWildcard); ok {
		return marshaler
	}
	if marshaler, ok := m.lookup(builtinJSONMIME); ok {
		return marshaler
	}
	return defaultMarshaler
}

// acceptRange is one media range of an Accept header, with its quality.
type acceptRange struct {
	raw       string
	mediaType string
	q         float64
}

func (a acceptRange) wildcard() bool {
	return a.mediaType == "*/*" || a.mediaType == "*" ||
		strings.HasSuffix(a.mediaType, "/*")
}

// outboundForRequest resolves the Accept header. A whole header value that
// exactly matches a registry key still wins without parsing, which is the
// common single-value case. Otherwise media ranges are considered by
// descending quality, and equal qualities keep their header order. Wildcard
// ranges are skipped: a client that accepts anything keeps the inbound
// marshaler rather than being switched to the MIMEWildcard registration.
func (m *marshalerSnapshot) outboundForRequest(r *http.Request) Marshaler {
	acceptValues := r.Header[acceptHeader]
	for _, acceptVal := range acceptValues {
		if marshaler, ok := m.lookup(acceptVal); ok {
			return marshaler
		}
	}
	// Every registration is MIMEWildcard, which a media range never selects, so
	// parsing could not change the result each request already gets.
	if !m.hasExact {
		return nil
	}

	var ranges []acceptRange
	for _, acceptVal := range acceptValues {
		for _, part := range strings.Split(acceptVal, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			ranges = append(ranges, parseAcceptRange(part))
		}
	}
	// SliceStable keeps header order between ranges of equal quality.
	sort.SliceStable(ranges, func(i, j int) bool { return ranges[i].q > ranges[j].q })

	for _, ar := range ranges {
		// q=0 means "not acceptable"; it must not select a marshaler.
		if ar.q <= 0 || ar.wildcard() {
			continue
		}
		if marshaler, ok := m.lookup(ar.raw); ok {
			return marshaler
		}
		if marshaler, ok := m.lookup(ar.mediaType); ok {
			return marshaler
		}
	}
	return nil
}

func parseAcceptRange(value string) acceptRange {
	if semi := strings.IndexByte(value, ';'); semi < 0 {
		// No parameters, so the quality is 1 and the media type only needs
		// lowercasing. This keeps the common "*/*" and bare-type headers off
		// mime.ParseMediaType, which allocates a parameter map.
		return acceptRange{raw: value, mediaType: strings.ToLower(value), q: 1}
	}
	mediaType, params, err := mime.ParseMediaType(value)
	if err != nil {
		// An unparseable range cannot select anything. Keep it at the end and
		// let the remaining ranges decide.
		return acceptRange{raw: value, q: -1}
	}
	q := 1.0
	if qs, ok := params["q"]; ok {
		if parsed, err := strconv.ParseFloat(qs, 64); err == nil {
			q = parsed
		}
	}
	return acceptRange{raw: value, mediaType: mediaType, q: q}
}

// marshalWithFallback encodes v with preferred. Only if that fails does it try
// the snapshot's compatible fallbacks, so the common path stays a single call.
// It returns the marshaler that produced the bytes so callers can report the
// matching Content-Type; if every candidate fails, it returns the last error.
func (m *marshalerSnapshot) marshalWithFallback(preferred Marshaler, v interface{}) ([]byte, Marshaler, error) {
	if preferred == nil {
		preferred = m.fallbackMarshaler()
	}
	buf, err := preferred.Marshal(v)
	if err == nil {
		return buf, preferred, nil
	}
	lastErr := err
	for _, marshaler := range m.fallbackCandidates(preferred) {
		buf, err := marshaler.Marshal(v)
		if err == nil {
			return buf, marshaler, nil
		}
		lastErr = err
	}
	return nil, preferred, lastErr
}

// fallbackCandidates returns the marshalers to try after preferred failed, in a
// deterministic order: the built-in JSON registration, the wildcard
// registration, and finally the package default. All but the last come from
// this snapshot, so a registration made after the request entered is never
// used as a fallback for it. Duplicate marshalers are dropped.
func (m *marshalerSnapshot) fallbackCandidates(preferred Marshaler) []Marshaler {
	candidates := make([]Marshaler, 0, 3)
	add := func(marshaler Marshaler) {
		if marshaler == nil || sameMarshaler(preferred, marshaler) {
			return
		}
		for _, existing := range candidates {
			if sameMarshaler(existing, marshaler) {
				return
			}
		}
		candidates = append(candidates, marshaler)
	}
	if marshaler, ok := m.lookup(builtinJSONMIME); ok {
		add(marshaler)
	}
	if marshaler, ok := m.lookup(MIMEWildcard); ok {
		add(marshaler)
	}
	add(defaultMarshaler)
	return candidates
}

// sameMarshaler reports whether a and b are the same marshaller value. Marshalers
// are usually pointers; a value type that is not comparable simply never matches.
func sameMarshaler(a, b Marshaler) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	if !reflect.TypeOf(a).Comparable() {
		return false
	}
	return a == b
}

// currentMarshalerSnapshot returns the mux's current registry snapshot.
func (s *ServeMux) currentMarshalerSnapshot() *marshalerSnapshot {
	if snapshot := s.marshalers.Load(); snapshot != nil {
		return snapshot
	}
	return emptyMarshalerSnapshot
}

// RegisterMarshaler associates marshaler with a case-sensitive MIME type in
// the mux's registry, replacing an earlier registration for the same type.
// "*" matches any Content-Type that has no exact registration.
//
// It is safe to call while the mux is serving requests. Requests that have
// already entered ServeHTTP keep using the snapshot they started with; the new
// registration applies from the next request. Registering an empty MIME type or
// a nil marshaler returns an error and leaves the registry unchanged.
func (s *ServeMux) RegisterMarshaler(mime string, marshaler Marshaler) error {
	if len(mime) == 0 {
		return errors.New("empty MIME type")
	}
	if marshaler == nil {
		return errors.New("nil Marshaler")
	}

	s.marshalersMu.Lock()
	defer s.marshalersMu.Unlock()
	s.marshalers.Store(s.currentMarshalerSnapshot().with(mime, marshaler))
	return nil
}

// UnregisterMarshaler removes the marshaler registered for a case-sensitive
// MIME type. It reports whether a registration was removed. Removing a type
// only affects requests that enter the mux after the call; requests already in
// flight keep the snapshot they started with. A removed "*" falls back to the
// built-in JSON marshaler.
func (s *ServeMux) UnregisterMarshaler(mime string) bool {
	s.marshalersMu.Lock()
	defer s.marshalersMu.Unlock()
	next, ok := s.currentMarshalerSnapshot().without(mime)
	if !ok {
		return false
	}
	s.marshalers.Store(next)
	return true
}

// WithMarshalerOption returns a ServeMuxOption which associates inbound and outbound
// Marshalers to a MIME type in mux.
//
// The option is applied while the mux is constructed. To change registrations
// later, while the mux is serving, use RegisterMarshaler or UnregisterMarshaler.
func WithMarshalerOption(mime string, marshaler Marshaler) ServeMuxOption {
	return func(mux *ServeMux) {
		if marshaler == nil {
			// Earlier releases stored a nil marshaler, which every lookup
			// treated as absent. Removing the type keeps that behaviour.
			if len(mime) == 0 {
				panic(errors.New("empty MIME type"))
			}
			mux.UnregisterMarshaler(mime)
			return
		}
		if err := mux.RegisterMarshaler(mime, marshaler); err != nil {
			panic(err)
		}
	}
}
