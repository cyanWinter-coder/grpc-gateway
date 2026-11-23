package runtime_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime/internal/examplepb"
	"github.com/grpc-ecosystem/grpc-gateway/v2/utilities"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func newQueryRequest(t *testing.T, rawQuery string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://example.com/?"+rawQuery, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	return req
}

func TestParseQueryParametersOrder(t *testing.T) {
	for _, spec := range []struct {
		name  string
		query string
		want  []runtime.QueryParameter
	}{
		{
			name:  "empty",
			query: "",
			want:  nil,
		},
		{
			name:  "single",
			query: "a=1",
			want:  []runtime.QueryParameter{{Key: "a", Value: "1"}},
		},
		{
			name:  "repeated key keeps order",
			query: "a=1&b=2&a=3",
			want: []runtime.QueryParameter{
				{Key: "a", Value: "1"},
				{Key: "b", Value: "2"},
				{Key: "a", Value: "3"},
			},
		},
		{
			name:  "key without value",
			query: "a&b=",
			want: []runtime.QueryParameter{
				{Key: "a", Value: ""},
				{Key: "b", Value: ""},
			},
		},
		{
			name:  "percent decoding",
			query: "map%5Bkey%5D=hello%20world",
			want:  []runtime.QueryParameter{{Key: "map[key]", Value: "hello world"}},
		},
		{
			name:  "empty pairs are skipped",
			query: "a=1&&b=2&",
			want: []runtime.QueryParameter{
				{Key: "a", Value: "1"},
				{Key: "b", Value: "2"},
			},
		},
	} {
		t.Run(spec.name, func(t *testing.T) {
			got, err := runtime.ParseQueryParameters(spec.query)
			if err != nil {
				t.Fatalf("ParseQueryParameters(%q) failed with %v", spec.query, err)
			}
			if len(got) != len(spec.want) {
				t.Fatalf("ParseQueryParameters(%q) = %v; want %v", spec.query, got, spec.want)
			}
			for i := range got {
				if got[i].Key != spec.want[i].Key || got[i].Value != spec.want[i].Value {
					t.Errorf("ParseQueryParameters(%q)[%d] = (%q, %q); want (%q, %q)",
						spec.query, i, got[i].Key, got[i].Value, spec.want[i].Key, spec.want[i].Value)
				}
			}
		})
	}

	t.Run("malformed escape", func(t *testing.T) {
		if _, err := runtime.ParseQueryParameters("a=%zz"); err == nil {
			t.Errorf("ParseQueryParameters(%q) succeeded; want an error", "a=%zz")
		}
	})
}

func TestPopulateQueryParametersFromRequestRepeatedOrder(t *testing.T) {
	// Repeated keys accumulate in wire order, including when the same field is
	// addressed by both its proto name and its JSON name.
	msg := &examplepb.Proto3Message{}
	req := newQueryRequest(t, "repeated_value=a&repeatedValue=b&repeated_value=c")
	paths, err := runtime.PopulateQueryParametersFromRequest(msg, req, utilities.NewDoubleArray(nil))
	if err != nil {
		t.Fatalf("PopulateQueryParametersFromRequest failed with %v", err)
	}
	want := &examplepb.Proto3Message{RepeatedValue: []string{"a", "b", "c"}}
	if diff := cmp.Diff(want, msg, protocmp.Transform()); diff != "" {
		t.Errorf("bound message: %s", diff)
	}
	if diff := cmp.Diff([]string{"repeated_value", "repeated_value", "repeated_value"}, paths); diff != "" {
		t.Errorf("bound paths: %s", diff)
	}
}

func TestPopulateQueryParametersFromRequestDuplicateScalar(t *testing.T) {
	// The same scalar key twice is an explicit error rather than a last-write
	// wins race, no matter how many other parameters surround it.
	msg := &examplepb.Proto3Message{}
	req := newQueryRequest(t, "string_value=a&bool_value=true&string_value=b")
	_, err := runtime.PopulateQueryParametersFromRequest(msg, req, utilities.NewDoubleArray(nil))
	if err == nil || !strings.Contains(err.Error(), `duplicate field "string_value"`) {
		t.Fatalf("PopulateQueryParametersFromRequest = %v; want duplicate field error", err)
	}
	if msg.StringValue != "" || msg.BoolValue {
		t.Errorf("message was mutated before the error: %v", msg)
	}
}

func TestPopulateQueryParametersFromRequestBadValueIsAtomic(t *testing.T) {
	// Every parameter decodes before any field is replaced, so a bad value
	// leaves the target message completely untouched and earlier parameters do
	// not partially land.
	for _, spec := range []struct {
		name  string
		query string
		// part of the expected error message
		wantErr string
	}{
		{
			name:    "bad bool carries its parameter path",
			query:   "nested.bool_value=maybe",
			wantErr: `invalid query parameter "nested.bool_value": parsing field "nested.bool_value"`,
		},
		{
			name:    "bad enum carries its parameter path",
			query:   "enum_value=NOPE",
			wantErr: `invalid query parameter "enum_value": parsing field "enum_value"`,
		},
		{
			name:    "bad number carries its parameter path",
			query:   "nested.nested.int32_value=1.5",
			wantErr: `invalid query parameter "nested.nested.int32_value"`,
		},
		{
			name:    "bad bool under json spelling",
			query:   "nested.boolValue=maybe",
			wantErr: `invalid query parameter "nested.boolValue"`,
		},
		{
			name:    "nested map bad value",
			query:   "nested.map_value2[k]=not-an-int",
			wantErr: `invalid query parameter "nested.map_value2[k]"`,
		},
	} {
		t.Run(spec.name, func(t *testing.T) {
			msg := &examplepb.Proto3Message{}
			req := newQueryRequest(t, "string_value=ok&"+spec.query)
			_, err := runtime.PopulateQueryParametersFromRequest(msg, req, utilities.NewDoubleArray(nil))
			if err == nil || !strings.Contains(err.Error(), spec.wantErr) {
				t.Fatalf("PopulateQueryParametersFromRequest = %v; want error containing %q", err, spec.wantErr)
			}
			// No internal type addresses leak into the error; users see field paths.
			if strings.Contains(err.Error(), "0x") {
				t.Errorf("error leaks a pointer address: %v", err)
			}
			if diff := cmp.Diff(&examplepb.Proto3Message{}, msg, protocmp.Transform()); diff != "" {
				t.Errorf("target message changed despite the error: %s", diff)
			}
		})
	}
}

func TestPopulateQueryParametersFromRequestNestedMessages(t *testing.T) {
	// Map and nested message fields are replaced only after decoding succeeds.
	// Sibling parameters that build one nested message merge instead of
	// overwriting each other.
	msg := &examplepb.Proto3Message{}
	req := newQueryRequest(t,
		"nested.string_value=u&"+
			"nested.nested.string_value=t&"+
			"nested.nested.nested.string_value=s&"+
			"nested.nested.map_value[first]=foo&"+
			"nested.nested.map_value[second]=bar")
	paths, err := runtime.PopulateQueryParametersFromRequest(msg, req, utilities.NewDoubleArray(nil))
	if err != nil {
		t.Fatalf("PopulateQueryParametersFromRequest failed with %v", err)
	}
	want := &examplepb.Proto3Message{
		Nested: &examplepb.Proto3Message{
			StringValue: "u",
			Nested: &examplepb.Proto3Message{
				StringValue: "t",
				MapValue:    map[string]string{"first": "foo", "second": "bar"},
				Nested: &examplepb.Proto3Message{
					StringValue: "s",
				},
			},
		},
	}
	if diff := cmp.Diff(want, msg, protocmp.Transform()); diff != "" {
		t.Errorf("bound message: %s", diff)
	}
	wantPaths := []string{
		"nested.string_value",
		"nested.nested.string_value",
		"nested.nested.nested.string_value",
		"nested.nested.map_value",
		"nested.nested.map_value",
	}
	if diff := cmp.Diff(wantPaths, paths); diff != "" {
		t.Errorf("bound paths: %s", diff)
	}
}

func TestPopulateQueryParametersFromRequestDuplicateMapKey(t *testing.T) {
	// The same map entry named twice fails, and the failure is atomic: neither
	// entry lands.
	msg := &examplepb.Proto3Message{}
	req := newQueryRequest(t, "map_value[key]=a&map_value[key]=b")
	_, err := runtime.PopulateQueryParametersFromRequest(msg, req, utilities.NewDoubleArray(nil))
	if err == nil || !strings.Contains(err.Error(), `duplicate key "key"`) {
		t.Fatalf("PopulateQueryParametersFromRequest = %v; want duplicate key error", err)
	}
	if msg.MapValue != nil {
		t.Errorf("map was mutated before the error: %v", msg.MapValue)
	}
}

func TestPopulateQueryParametersFromRequestRepeatedRequestsAreStable(t *testing.T) {
	// Replaying the same request twice, and parsing its raw query versus its
	// url.Values form, must produce the same bound message. This guards against
	// map iteration order deciding which value survives.
	rawQuery := "repeated_enum=X&repeated_enum=2&repeated_enum=0&string_value=str"
	want := &examplepb.Proto3Message{
		StringValue:  "str",
		RepeatedEnum: []examplepb.EnumValue{examplepb.EnumValue_X, examplepb.EnumValue_Z, examplepb.EnumValue_X},
	}

	for i := 0; i < 10; i++ {
		ordered := &examplepb.Proto3Message{}
		if _, err := runtime.PopulateQueryParametersFromRequest(ordered, newQueryRequest(t, rawQuery), utilities.NewDoubleArray(nil)); err != nil {
			t.Fatalf("iteration %d: ordered parse failed with %v", i, err)
		}
		if diff := cmp.Diff(want, ordered, protocmp.Transform()); diff != "" {
			t.Fatalf("iteration %d: ordered bind mismatch: %s", i, diff)
		}

		values, err := url.ParseQuery(rawQuery)
		if err != nil {
			t.Fatalf("url.ParseQuery: %v", err)
		}
		unordered := &examplepb.Proto3Message{}
		if err := runtime.PopulateQueryParameters(unordered, values, utilities.NewDoubleArray(nil)); err != nil {
			t.Fatalf("iteration %d: url.Values parse failed with %v", i, err)
		}
		if !proto.Equal(ordered, unordered) {
			t.Fatalf("iteration %d: ordered %v != unordered %v", i, ordered, unordered)
		}
	}
}

func TestPopulateQueryParametersFromRequestFiltersAndUnknownFields(t *testing.T) {
	msg := &examplepb.Proto3Message{}
	req := newQueryRequest(t, "bool_value=true&string_value=str&not_a_field=ignored")
	filter := utilities.NewDoubleArray([][]string{{"bool_value"}})
	paths, err := runtime.PopulateQueryParametersFromRequest(msg, req, filter)
	if err != nil {
		t.Fatalf("PopulateQueryParametersFromRequest failed with %v", err)
	}
	want := &examplepb.Proto3Message{StringValue: "str"}
	if diff := cmp.Diff(want, msg, protocmp.Transform()); diff != "" {
		t.Errorf("bound message: %s", diff)
	}
	// Filtered fields and unknown fields contribute no mask paths.
	if diff := cmp.Diff([]string{"string_value"}, paths); diff != "" {
		t.Errorf("bound paths: %s", diff)
	}
}
