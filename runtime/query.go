package runtime

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/utilities"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	field_mask "google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// splitMapKey splits a query parameter name of the form "field[key]" into the part before the
// bracket and the key inside it, reporting whether the name has that shape at all.
//
// This replaces a FindStringSubmatch against `^(.*)\[(.*)\]$`, which ran for every query
// parameter and cost two allocations each -- the submatch slice and the backtracker's state --
// to answer a question that is two index operations. The returned strings are slices of the
// caller's key, so nothing is copied; the caller rebuilds "[key]" as key[len(prefix):] rather
// than concatenating it back together.
//
// The regexp is matched exactly, including two details worth spelling out:
//
//   - The first group is greedy, so the bracket that counts is the last one before the final
//     "]". "a[b]c]" splits into "a" and "b]c", which is what LastIndexByte over everything but
//     the final byte finds.
//   - Go's regexp does not let "." match a newline and anchors ^ and $ at the ends of the text,
//     so a key containing a newline could never match the pattern no matter where the brackets
//     sat. The explicit newline check keeps that, rather than quietly starting to treat
//     "a\n[b]" as a map key.
func splitMapKey(key string) (prefix, inner string, ok bool) {
	if len(key) < 2 || key[len(key)-1] != ']' || strings.IndexByte(key, '\n') >= 0 {
		return "", "", false
	}
	open := strings.LastIndexByte(key[:len(key)-1], '[')
	if open < 0 {
		return "", "", false
	}
	return key[:open], key[open+1 : len(key)-1], true
}

// QueryParameter is one query parameter as it appeared in the request: a key
// as written by the client and the single value paired with it. Repeated keys
// show up as one QueryParameter per occurrence, in wire order, so a parser can
// tell "a=1&a=2" (one key appearing twice) from "a=1&b=2" (two keys) and keep
// the order in which parameters were sent.
type QueryParameter struct {
	Key   string
	Value string
	// sameKey groups values that url.Values stored under one map key, i.e. the
	// values of one "a=1&a=2" pair. A second singular write within a group is
	// the legacy "too many values" shape; across groups it is a duplicate field.
	sameKey bool
}

// ParseQueryParameters splits a URL query string (the part after '?', without
// the leading '?') into parameters in wire order. A key may appear more than
// once; each occurrence is returned separately. It is the ordered counterpart
// of url.ParseQuery, which collapses repeated keys into an unordered map.
//
// Malformed percent-escapes and keys containing a NUL byte are rejected with
// the same errors url.ParseQuery returns.
func ParseQueryParameters(query string) ([]QueryParameter, error) {
	if query == "" {
		return nil, nil
	}
	params := make([]QueryParameter, 0, strings.Count(query, "&")+1)
	for _, pair := range strings.Split(query, "&") {
		if pair == "" {
			continue
		}
		rawKey, rawValue, hasValue := pair, "", false
		if i := strings.IndexByte(pair, '='); i >= 0 {
			rawKey, rawValue, hasValue = pair[:i], pair[i+1:], true
		}
		key, err := url.QueryUnescape(rawKey)
		if err != nil {
			return nil, err
		}
		if strings.ContainsRune(key, 0) {
			return nil, errors.New("net/url: invalid userinfo")
		}
		var value string
		if hasValue {
			value, err = url.QueryUnescape(rawValue)
			if err != nil {
				return nil, err
			}
		}
		params = append(params, QueryParameter{Key: key, Value: value})
	}
	return params, nil
}

// queryValuesFromURLValues flattens an unordered url.Values into the ordered
// parameter representation. The relative order of distinct keys follows their
// sorted names, matching the deterministic order of url.Values.Encode, while
// the values stored under one key keep their slice order. It keeps the legacy
// Parse(msg, url.Values, ...) entry point deterministic.
func queryValuesFromURLValues(values url.Values) []QueryParameter {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	params := make([]QueryParameter, 0, len(values))
	for _, key := range keys {
		for i, value := range values[key] {
			params = append(params, QueryParameter{Key: key, Value: value, sameKey: i > 0})
		}
	}
	return params
}

var currentQueryParser QueryParameterParser = &DefaultQueryParser{}

// QueryParameterParser defines interface for all query parameter parsers
type QueryParameterParser interface {
	Parse(msg proto.Message, values url.Values, filter *utilities.DoubleArray) error
}

// PopulateQueryParameters parses query parameters
// into "msg" using current query parser
func PopulateQueryParameters(msg proto.Message, values url.Values, filter *utilities.DoubleArray) error {
	return currentQueryParser.Parse(msg, values, filter)
}

// PopulateQueryParametersFromRequest parses the request's query parameters into
// msg in wire order. Unlike PopulateQueryParameters, which takes the parsed
// url.Values map, repeated keys stay distinct and ordered, so repeated fields
// accumulate in the order the client sent them and every error names the
// parameter as the client wrote it. It also returns the normalized field paths
// it bound, one per parameter that named a field and was not dropped by the
// filter, for building a field mask over exactly the fields the request set.
//
// No field of msg is replaced until every parameter has decoded, so a bad
// parameter leaves msg untouched rather than half populated.
func PopulateQueryParametersFromRequest(msg proto.Message, req *http.Request, filter *utilities.DoubleArray) ([]string, error) {
	params, err := ParseQueryParameters(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	return (&DefaultQueryParser{}).parseOrdered(msg, params, filter, true)
}

// DefaultQueryParser is a QueryParameterParser which implements the default
// query parameters parsing behavior.
//
// See https://github.com/grpc-ecosystem/grpc-gateway/issues/2632 for more context.
type DefaultQueryParser struct{}

// Parse populates "values" into "msg".
// A value is ignored if its key starts with one of the elements in "filter",
// or names a field that contains one of them.
//
// Parse works from url.Values, a map, so the order in which distinct keys are
// applied is not the client's wire order. Code that has the original
// *http.Request should call PopulateQueryParametersFromRequest instead, which
// parses the raw query in wire order and reports the bound field paths.
func (*DefaultQueryParser) Parse(msg proto.Message, values url.Values, filter *utilities.DoubleArray) error {
	_, err := (&DefaultQueryParser{}).ParseOrdered(msg, queryValuesFromURLValues(values), filter)
	return err
}

// ParseOrdered populates the ordered parameters into msg and returns the
// normalized field paths it bound (one per parameter that named a field and was
// not dropped by the filter). A value is ignored if its key starts with one of
// the elements in filter, or names a field that contains one of them.
//
// No field of msg is replaced until every parameter has decoded successfully,
// so a bad parameter leaves msg untouched rather than half populated.
func (*DefaultQueryParser) ParseOrdered(msg proto.Message, params []QueryParameter, filter *utilities.DoubleArray) ([]string, error) {
	return (&DefaultQueryParser{}).parseOrdered(msg, params, filter, false)
}

func (*DefaultQueryParser) parseOrdered(msg proto.Message, params []QueryParameter, filter *utilities.DoubleArray, wrapParamErrors bool) ([]string, error) {
	b := newQueryBinder(msg.ProtoReflect())
	var maskPaths []string
	for _, param := range params {
		fieldKey, mapKey, mapKeyInner := param.Key, "", ""
		if prefix, inner, ok := splitMapKey(param.Key); ok {
			fieldKey, mapKey, mapKeyInner = prefix, param.Key[len(prefix):], inner
		}

		fieldPath := normalizeFieldPath(b.target, strings.Split(fieldKey, "."))
		if filter.HasCommonPrefix(fieldPath) || containsFilteredField(filter, fieldPath) {
			continue
		}
		// A key that names no field is ignored, for compatibility with requests
		// that carry extra, non-schema query parameters. It contributes no mask
		// path either: the mask has to match fields the request actually bound.
		// A key whose path descends through a scalar or repeated segment names a
		// real prefix but not a real field: that is a malformed path, not an
		// extra parameter, so let resolve report it.
		if !fieldExists(b.target, fieldPath) && !hasFieldPrefix(b.target, fieldPath) {
			grpclog.Infof("field not found in %q: %q", b.target.Descriptor().FullName(), fieldKey)
			continue
		}

		if err := b.bind(fieldPath, param.Value, mapKey, mapKeyInner, param.sameKey); err != nil {
			if wrapParamErrors {
				return nil, fmt.Errorf("invalid query parameter %q: %w", param.Key, err)
			}
			return nil, err
		}
		maskPaths = append(maskPaths, strings.Join(fieldPath, "."))
	}
	if err := b.commit(); err != nil {
		if wrapParamErrors {
			return nil, fmt.Errorf("invalid query string: %w", err)
		}
		return nil, err
	}
	return maskPaths, nil
}

// containsFilteredField reports whether fieldPath names a field that contains
// one of the sequences in filter, such as "id" for a path parameter bound to
// "id.value". Setting such a field from the query, which the parser can do for
// well-known types like wrappers and timestamps, would replace the whole
// message and with it the value already bound from the path or body.
func containsFilteredField(filter *utilities.DoubleArray, fieldPath []string) bool {
	if len(filter.Base) == 0 {
		return false
	}

	var i int
	for _, t := range fieldPath {
		code, ok := filter.Encoding[t]
		if !ok {
			return false
		}
		j := filter.Base[i] + code
		if len(filter.Check) <= j || filter.Check[j] != i+1 {
			return false
		}
		i = j
	}
	return true
}

// fieldExists reports whether fieldPath resolves through messages to a field.
// normalizeFieldPath returns the input unchanged when resolution fails, so the
// caller cannot otherwise tell a real field from a typo.
func fieldExists(msgValue protoreflect.Message, fieldPath []string) bool {
	for i, fieldName := range fieldPath {
		fd := msgValue.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
		if fd == nil {
			return false
		}
		if i == len(fieldPath)-1 {
			return true
		}
		// A path can only descend through singular message fields; a scalar or
		// repeated segment cannot have a child.
		if fd.Message() == nil || fd.Cardinality() == protoreflect.Repeated {
			return false
		}
		msgValue = msgValue.Get(fd).Message()
	}
	return false
}

// hasFieldPrefix reports whether some prefix of fieldPath names a real field.
// A key that shares no field at all is an unrelated extra parameter and is
// ignored; a key like "scalar.child" names a real field ("scalar") but cannot
// descend into it, which is a malformed path error rather than an ignore.
func hasFieldPrefix(msgValue protoreflect.Message, fieldPath []string) bool {
	for _, fieldName := range fieldPath {
		fd := msgValue.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
		if fd == nil {
			return false
		}
		if fd.Message() == nil || fd.Cardinality() == protoreflect.Repeated {
			return true
		}
		msgValue = msgValue.Get(fd).Message()
	}
	return true
}

// PopulateFieldFromPath sets a value in a nested Protobuf structure.
func PopulateFieldFromPath(msg proto.Message, fieldPathString string, value string) error {
	fieldPath := strings.Split(fieldPathString, ".")
	return populateFieldValueFromPath(msg.ProtoReflect(), fieldPath, []string{value}, false)
}

func normalizeFieldPath(msgValue protoreflect.Message, fieldPath []string) []string {
	newFieldPath := make([]string, 0, len(fieldPath))
	for i, fieldName := range fieldPath {
		fields := msgValue.Descriptor().Fields()
		fieldDesc := fields.ByTextName(fieldName)
		if fieldDesc == nil {
			fieldDesc = fields.ByJSONName(fieldName)
		}
		if fieldDesc == nil {
			// return initial field path values if no matching  message field was found
			return fieldPath
		}

		newFieldPath = append(newFieldPath, string(fieldDesc.Name()))

		// If this is the last element, we're done
		if i == len(fieldPath)-1 {
			break
		}

		// Only singular message fields are allowed
		if fieldDesc.Message() == nil || fieldDesc.Cardinality() == protoreflect.Repeated {
			return fieldPath
		}

		// Get the nested message
		msgValue = msgValue.Get(fieldDesc).Message()
	}

	return newFieldPath
}

// queryBinder accumulates the decoded values of one parse and applies them to
// the target message only once every parameter has decoded. bind* methods
// mutate private scratch state; commit is the single place that touches the
// caller's message, so a parse error can never leave it half populated.
type queryBinder struct {
	target protoreflect.Message

	// Scalars and singular message fields, keyed by the full dotted field path.
	// A second singular write to the same path is a duplicate-field error.
	singular map[string]protoreflect.Value
	// Repeated scalar/message fields, accumulated in parameter order.
	lists map[string][]protoreflect.Value
	// Singular message fields reached through a dotted path: their scratch
	// message, so sibling parameters (e.g. "nested.a=1&nested.b=2") build one
	// message together rather than overwriting each other.
	messages map[string]protoreflect.Message
	// Map fields: scratch map plus the keys already written, to refuse a
	// duplicate map entry regardless of key spelling. MapKey holds an
	// interface and cannot key a Go map itself, so keys are stored as their
	// canonical string form.
	maps      map[string]protoreflect.Map
	mapKeys   map[string]map[string]struct{}
	mapFields map[string]protoreflect.FieldDescriptor
	// Oneofs already claimed, so two parameters cannot set different fields of
	// the same non-synthetic oneof.
	oneofs map[string]protoreflect.FieldDescriptor

	// Bare map spelling ("m=k&m=v", without brackets): parameters for one map
	// field alternate key,value. Holds a key waiting for its value, per path.
	pendingMapKey map[string]protoreflect.MapKey
}

func newQueryBinder(target protoreflect.Message) *queryBinder {
	return &queryBinder{
		target:        target,
		singular:      make(map[string]protoreflect.Value),
		lists:         make(map[string][]protoreflect.Value),
		messages:      make(map[string]protoreflect.Message),
		maps:          make(map[string]protoreflect.Map),
		mapKeys:       make(map[string]map[string]struct{}),
		mapFields:     make(map[string]protoreflect.FieldDescriptor),
		oneofs:        make(map[string]protoreflect.FieldDescriptor),
		pendingMapKey: make(map[string]protoreflect.MapKey),
	}
}

// resolve walks fieldPath from the binder's scratch messages, creating scratch
// singular messages on the way, and returns the message and field descriptor
// the path ends at. Parent paths are scratch messages and only reach the
// target in commit.
func (b *queryBinder) resolve(fieldPath []string) (protoreflect.Message, protoreflect.FieldDescriptor, error) {
	msgValue := b.target
	for i, fieldName := range fieldPath {
		fd := msgValue.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
		if fd == nil {
			fd = msgValue.Descriptor().Fields().ByJSONName(fieldName)
			if fd == nil {
				return nil, nil, fmt.Errorf("field %q not found", strings.Join(fieldPath, "."))
			}
		}

		if of := fd.ContainingOneof(); of != nil && !of.IsSynthetic() {
			path := strings.Join(fieldPath[:i], ".")
			if path != "" {
				path += "."
			}
			oneofPath := path + string(of.Name())
			if existing, ok := b.oneofs[oneofPath]; ok && existing.FullName() != fd.FullName() {
				return nil, nil, fmt.Errorf("field already set for oneof %q", of.Name())
			}
			b.oneofs[oneofPath] = fd
		}

		if i == len(fieldPath)-1 {
			return msgValue, fd, nil
		}
		if fd.Message() == nil || fd.Cardinality() == protoreflect.Repeated {
			return nil, nil, fmt.Errorf("invalid path: %q is not a message", fieldName)
		}

		childPath := strings.Join(fieldPath[:i+1], ".")
		scratch, ok := b.messages[childPath]
		if !ok {
			scratch = msgValue.NewField(fd).Message()
			b.messages[childPath] = scratch
		}
		msgValue = scratch
	}
	return nil, nil, errors.New("no field path")
}

func (b *queryBinder) bind(fieldPath []string, value, mapKey, mapKeyInner string, sameKey bool) error {
	msgValue, fd, err := b.resolve(fieldPath)
	if err != nil {
		return err
	}
	path := strings.Join(fieldPath, ".")

	// A key using the map syntax (field[key]=value) must name a map field,
	// or a repeated field of key/value messages laid out like map entries.
	// Otherwise the bracket content would be bound as if it were a regular
	// value: appended as a phantom element of a repeated field, or counted
	// as a second value for a singular one.
	if mapKey != "" && !fd.IsMap() {
		if isKeyValueList(fd) {
			return b.bindKeyValueList(fd, msgValue, path, mapKeyInner, value)
		}
		return fmt.Errorf("field %q is not a map", path)
	}

	switch {
	case fd.IsMap():
		return b.bindMap(fd, path, mapKey, mapKeyInner, value)
	case fd.IsList():
		v, err := parseField(fd, value)
		if err != nil {
			return fmt.Errorf("parsing list %q: %w", path, err)
		}
		b.lists[path] = append(b.lists[path], v)
		return nil
	}

	v, err := parseField(fd, value)
	if err != nil {
		return fmt.Errorf("parsing field %q: %w", path, err)
	}
	if _, ok := b.singular[path]; ok {
		if sameKey {
			return fmt.Errorf("too many values for field %q", path)
		}
		return fmt.Errorf("duplicate field %q", path)
	}
	b.singular[path] = v
	return nil
}

func (b *queryBinder) bindKeyValueList(fd protoreflect.FieldDescriptor, msgValue protoreflect.Message, path, keyText, value string) error {
	fields := fd.Message().Fields()
	key, err := parseField(fields.ByName("key"), keyText)
	if err != nil {
		return fmt.Errorf("parsing map key in %q: %w", path, err)
	}
	v, err := parseField(fields.ByName("value"), value)
	if err != nil {
		return fmt.Errorf("parsing list %q: %w", path, err)
	}
	entry := msgValue.NewField(fd).List().NewElement()
	entry.Message().Set(fields.ByName("key"), key)
	entry.Message().Set(fields.ByName("value"), v)
	b.lists[path] = append(b.lists[path], entry)
	return nil
}

func (b *queryBinder) bindMap(fd protoreflect.FieldDescriptor, path, mapKey, mapKeyInner, value string) error {
	mp, ok := b.maps[path]
	if !ok {
		mp = b.target.NewField(fd).Map()
		b.maps[path] = mp
		b.mapKeys[path] = make(map[string]struct{})
		b.mapFields[path] = fd
	}

	// The bare "m=k&m=v" spelling alternates key and value parameters on the
	// same path; the bracket spelling carries key and value in one parameter.
	if mapKey == "" {
		if pending, waiting := b.pendingMapKey[path]; waiting {
			val, err := parseField(fd.MapValue(), value)
			if err != nil {
				return fmt.Errorf("parsing map value in %q: %w", path, err)
			}
			mp.Set(pending, val)
			delete(b.pendingMapKey, path)
			return nil
		}
		key, err := parseField(fd.MapKey(), value)
		if err != nil {
			return fmt.Errorf("parsing map key in %q: %w", path, err)
		}
		mk := key.MapKey()
		if _, dup := b.mapKeys[path][mk.String()]; dup {
			return fmt.Errorf("duplicate key %q in map %q", mk.String(), fd.FullName())
		}
		b.mapKeys[path][mk.String()] = struct{}{}
		b.pendingMapKey[path] = mk
		return nil
	}

	key, err := parseField(fd.MapKey(), mapKeyInner)
	if err != nil {
		return fmt.Errorf("parsing map key in %q: %w", path, err)
	}
	mk := key.MapKey()
	if _, dup := b.mapKeys[path][mk.String()]; dup {
		return fmt.Errorf("duplicate key %q in map %q", mk.String(), fd.FullName())
	}
	val, err := parseField(fd.MapValue(), value)
	if err != nil {
		return fmt.Errorf("parsing map value in %q: %w", path, err)
	}
	b.mapKeys[path][mk.String()] = struct{}{}
	mp.Set(mk, val)
	return nil
}

// commit applies every decoded value to the target message. Scratch messages
// are merged deepest first, so nested parameters land on the target exactly
// once and never clobber each other.
func (b *queryBinder) commit() error {
	// A bare-spelling key left without a following value never formed an entry.
	for path, mk := range b.pendingMapKey {
		return fmt.Errorf("missing value for key %q in map %q", mk.String(), b.mapFields[path].FullName())
	}

	messagePaths := make([]string, 0, len(b.messages))
	for path := range b.messages {
		messagePaths = append(messagePaths, path)
	}
	sort.Slice(messagePaths, func(i, j int) bool {
		// More dotted segments first; ties do not matter because distinct
		// branches share no parent field.
		return strings.Count(messagePaths[i], ".") > strings.Count(messagePaths[j], ".")
	})
	for _, path := range messagePaths {
		if err := b.commitMessage(path); err != nil {
			return err
		}
	}

	for path, v := range b.singular {
		msgValue, fd, err := b.locate(strings.Split(path, "."))
		if err != nil {
			return err
		}
		msgValue.Set(fd, v)
	}
	for path, values := range b.lists {
		msgValue, fd, err := b.locate(strings.Split(path, "."))
		if err != nil {
			return err
		}
		list := msgValue.Mutable(fd).List()
		for _, v := range values {
			list.Append(v)
		}
	}
	for path, mp := range b.maps {
		msgValue, fd, err := b.locate(strings.Split(path, "."))
		if err != nil {
			return err
		}
		targetMap := msgValue.Mutable(fd).Map()
		mp.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			targetMap.Set(k, v)
			return true
		})
	}
	return nil
}

// commitMessage merges one scratch singular message into its parent scratch
// message (or the target).
func (b *queryBinder) commitMessage(path string) error {
	parts := strings.Split(path, ".")
	parentPath := strings.Join(parts[:len(parts)-1], ".")
	fieldName := parts[len(parts)-1]

	parent := b.target
	if parentPath != "" {
		parent = b.messages[parentPath]
	}
	fd := parent.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fd == nil {
		return fmt.Errorf("field %q not found", path)
	}
	scratch := b.messages[path]
	if existing := parent.Get(fd); existing.Message().IsValid() {
		proto.Merge(existing.Message().Interface(), scratch.Interface())
	} else {
		parent.Set(fd, protoreflect.ValueOfMessage(scratch))
	}
	return nil
}

// locate resolves a committed field path against the target, descending into
// the messages that commit has already placed there.
func (b *queryBinder) locate(fieldPath []string) (protoreflect.Message, protoreflect.FieldDescriptor, error) {
	msgValue := b.target
	for i, fieldName := range fieldPath {
		fd := msgValue.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
		if fd == nil {
			return nil, nil, fmt.Errorf("field %q not found", strings.Join(fieldPath, "."))
		}
		if i == len(fieldPath)-1 {
			return msgValue, fd, nil
		}
		msgValue = msgValue.Mutable(fd).Message()
	}
	return nil, nil, errors.New("no field path")
}

func populateFieldValueFromPath(msgValue protoreflect.Message, fieldPath []string, values []string, hasMapKey bool) error {
	if len(fieldPath) < 1 {
		return errors.New("no field path")
	}
	if len(values) < 1 {
		return errors.New("no value provided")
	}

	var fieldDescriptor protoreflect.FieldDescriptor
	for i, fieldName := range fieldPath {
		fields := msgValue.Descriptor().Fields()

		// Get field by name
		fieldDescriptor = fields.ByName(protoreflect.Name(fieldName))
		if fieldDescriptor == nil {
			fieldDescriptor = fields.ByJSONName(fieldName)
			if fieldDescriptor == nil {
				// We're not returning an error here because this could just be
				// an extra query parameter that isn't part of the request.
				grpclog.Infof("field not found in %q: %q", msgValue.Descriptor().FullName(), strings.Join(fieldPath, "."))
				return nil
			}
		}

		// Check if oneof already set
		if of := fieldDescriptor.ContainingOneof(); of != nil && !of.IsSynthetic() {
			if f := msgValue.WhichOneof(of); f != nil {
				if fieldDescriptor.Message() == nil || fieldDescriptor.FullName() != f.FullName() {
					return fmt.Errorf("field already set for oneof %q", of.FullName().Name())
				}
			}
		}

		// If this is the last element, we're done
		if i == len(fieldPath)-1 {
			break
		}

		// Only singular message fields are allowed
		if fieldDescriptor.Message() == nil || fieldDescriptor.Cardinality() == protoreflect.Repeated {
			return fmt.Errorf("invalid path: %q is not a message", fieldName)
		}

		// Get the nested message
		msgValue = msgValue.Mutable(fieldDescriptor).Message()
	}

	// A key using the map syntax (field[key]=value) must name a map field,
	// or a repeated field of key/value messages laid out like map entries.
	// Otherwise the bracket content would be bound as if it were a regular
	// value: appended as a phantom element of a repeated field, or counted
	// as a second value for a singular one.
	if hasMapKey && !fieldDescriptor.IsMap() {
		if isKeyValueList(fieldDescriptor) {
			return populateKeyValueListField(fieldDescriptor, msgValue.Mutable(fieldDescriptor).List(), values)
		}
		return fmt.Errorf("field %q is not a map", fieldDescriptor.FullName().Name())
	}

	switch {
	case fieldDescriptor.IsList():
		return populateRepeatedField(fieldDescriptor, msgValue.Mutable(fieldDescriptor).List(), values)
	case fieldDescriptor.IsMap():
		return populateMapField(fieldDescriptor, msgValue.Mutable(fieldDescriptor).Map(), values)
	}

	if len(values) > 1 {
		return fmt.Errorf("too many values for field %q: %s", fieldDescriptor.FullName().Name(), strings.Join(values, ", "))
	}

	return populateField(fieldDescriptor, msgValue, values[0])
}

func populateField(fieldDescriptor protoreflect.FieldDescriptor, msgValue protoreflect.Message, value string) error {
	v, err := parseField(fieldDescriptor, value)
	if err != nil {
		return fmt.Errorf("parsing field %q: %w", fieldDescriptor.FullName().Name(), err)
	}

	msgValue.Set(fieldDescriptor, v)
	return nil
}

func populateRepeatedField(fieldDescriptor protoreflect.FieldDescriptor, list protoreflect.List, values []string) error {
	for _, value := range values {
		v, err := parseField(fieldDescriptor, value)
		if err != nil {
			return fmt.Errorf("parsing list %q: %w", fieldDescriptor.FullName().Name(), err)
		}
		list.Append(v)
	}

	return nil
}

func populateMapField(fieldDescriptor protoreflect.FieldDescriptor, mp protoreflect.Map, values []string) error {
	if len(values) != 2 {
		return fmt.Errorf("more than one value provided for key %q in map %q", values[0], fieldDescriptor.FullName())
	}

	key, err := parseField(fieldDescriptor.MapKey(), values[0])
	if err != nil {
		return fmt.Errorf("parsing map key %q: %w", fieldDescriptor.FullName().Name(), err)
	}
	if mp.Has(key.MapKey()) {
		return fmt.Errorf("duplicate key %q in map %q", key.MapKey().String(), fieldDescriptor.FullName())
	}

	value, err := parseField(fieldDescriptor.MapValue(), values[1])
	if err != nil {
		return fmt.Errorf("parsing map value %q: %w", fieldDescriptor.FullName().Name(), err)
	}

	mp.Set(key.MapKey(), value)

	return nil
}

// isKeyValueList reports whether fieldDescriptor is a repeated message field
// whose message has exactly a singular "key" and a singular "value" field.
// Such a field is shaped like a map entry list, but can hold more than one
// value for a key.
func isKeyValueList(fieldDescriptor protoreflect.FieldDescriptor) bool {
	if !fieldDescriptor.IsList() || fieldDescriptor.Message() == nil {
		return false
	}
	fields := fieldDescriptor.Message().Fields()
	if fields.Len() != 2 {
		return false
	}
	key, value := fields.ByName("key"), fields.ByName("value")
	return key != nil && key.Cardinality() != protoreflect.Repeated &&
		value != nil && value.Cardinality() != protoreflect.Repeated
}

// populateKeyValueListField appends one key/value message to list for each of
// values[1:], all with the key values[0].
func populateKeyValueListField(fieldDescriptor protoreflect.FieldDescriptor, list protoreflect.List, values []string) error {
	fields := fieldDescriptor.Message().Fields()
	keyField, valueField := fields.ByName("key"), fields.ByName("value")

	key, err := parseField(keyField, values[0])
	if err != nil {
		return fmt.Errorf("parsing map key %q: %w", fieldDescriptor.FullName().Name(), err)
	}

	for _, value := range values[1:] {
		v, err := parseField(valueField, value)
		if err != nil {
			return fmt.Errorf("parsing map value %q: %w", fieldDescriptor.FullName().Name(), err)
		}
		entry := list.NewElement()
		entry.Message().Set(keyField, key)
		entry.Message().Set(valueField, v)
		list.Append(entry)
	}

	return nil
}

func parseField(fieldDescriptor protoreflect.FieldDescriptor, value string) (protoreflect.Value, error) {
	switch fieldDescriptor.Kind() {
	case protoreflect.BoolKind:
		v, err := strconv.ParseBool(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfBool(v), nil
	case protoreflect.EnumKind:
		enum, err := protoregistry.GlobalTypes.FindEnumByName(fieldDescriptor.Enum().FullName())
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				return protoreflect.Value{}, fmt.Errorf("enum %q is not registered", fieldDescriptor.Enum().FullName())
			}
			return protoreflect.Value{}, fmt.Errorf("failed to look up enum: %w", err)
		}
		// Look for enum by name
		v := enum.Descriptor().Values().ByName(protoreflect.Name(value))
		if v == nil {
			i, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return protoreflect.Value{}, fmt.Errorf("%q is not a valid value", value)
			}
			// Look for enum by number
			if v = enum.Descriptor().Values().ByNumber(protoreflect.EnumNumber(i)); v == nil {
				return protoreflect.Value{}, fmt.Errorf("%q is not a valid value", value)
			}
		}
		return protoreflect.ValueOfEnum(v.Number()), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(v)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(v), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		v, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint32(uint32(v)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint64(v), nil
	case protoreflect.FloatKind:
		v, err := parseFloat(value, 32)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfFloat32(float32(v)), nil
	case protoreflect.DoubleKind:
		v, err := parseFloat(value, 64)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfFloat64(v), nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(value), nil
	case protoreflect.BytesKind:
		v, err := Bytes(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfBytes(v), nil
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return parseMessage(fieldDescriptor.Message(), value)
	default:
		panic(fmt.Sprintf("unknown field kind: %v", fieldDescriptor.Kind()))
	}
}

func parseMessage(msgDescriptor protoreflect.MessageDescriptor, value string) (protoreflect.Value, error) {
	var msg proto.Message
	switch msgDescriptor.FullName() {
	case "google.protobuf.Timestamp":
		t, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		timestamp := timestamppb.New(t)
		if ok := timestamp.IsValid(); !ok {
			return protoreflect.Value{}, fmt.Errorf("%s before 0001-01-01", value)
		}
		msg = timestamp
	case "google.protobuf.Duration":
		d, err := time.ParseDuration(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		msg = durationpb.New(d)
	case "google.protobuf.DoubleValue":
		v, err := parseFloat(value, 64)
		if err != nil {
			return protoreflect.Value{}, err
		}
		msg = wrapperspb.Double(v)
	case "google.protobuf.FloatValue":
		v, err := parseFloat(value, 32)
		if err != nil {
			return protoreflect.Value{}, err
		}
		msg = wrapperspb.Float(float32(v))
	case "google.protobuf.Int64Value":
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return protoreflect.Value{}, err
		}
		msg = wrapperspb.Int64(v)
	case "google.protobuf.Int32Value":
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return protoreflect.Value{}, err
		}
		msg = wrapperspb.Int32(int32(v))
	case "google.protobuf.UInt64Value":
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return protoreflect.Value{}, err
		}
		msg = wrapperspb.UInt64(v)
	case "google.protobuf.UInt32Value":
		v, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return protoreflect.Value{}, err
		}
		msg = wrapperspb.UInt32(uint32(v))
	case "google.protobuf.BoolValue":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		msg = wrapperspb.Bool(v)
	case "google.protobuf.StringValue":
		msg = wrapperspb.String(value)
	case "google.protobuf.BytesValue":
		v, err := Bytes(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		msg = wrapperspb.Bytes(v)
	case "google.protobuf.FieldMask":
		fm := &field_mask.FieldMask{}
		if value != "" {
			fm.Paths = append(fm.Paths, strings.Split(value, ",")...)
		}
		msg = fm
	case "google.protobuf.Value":
		var v structpb.Value
		if err := protojson.Unmarshal([]byte(value), &v); err != nil {
			return protoreflect.Value{}, err
		}
		msg = &v
	case "google.protobuf.Struct":
		var v structpb.Struct
		if err := protojson.Unmarshal([]byte(value), &v); err != nil {
			return protoreflect.Value{}, err
		}
		msg = &v
	case "google.protobuf.Empty":
		// An Empty carries nothing, so the only thing the value can say is that the
		// field is present. Accept the two spellings of "no content" and reject
		// anything else, rather than silently ignoring it.
		if value != "" && value != "{}" {
			return protoreflect.Value{}, fmt.Errorf("expected an empty value or %q, got %q", "{}", value)
		}
		msg = &emptypb.Empty{}
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported message type: %q", string(msgDescriptor.FullName()))
	}

	return protoreflect.ValueOfMessage(msg.ProtoReflect()), nil
}
