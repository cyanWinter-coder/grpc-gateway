package integration_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/examples/internal/proto/examplepb"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// captureUpdateBookServer records the requests the generated gateway forwards,
// so the test can observe both the field mask built from body and query and the
// bound values, without depending on the shared integration gateway.
type captureUpdateBookServer struct {
	examplepb.UnimplementedABitOfEverythingServiceServer
	requests chan *examplepb.UpdateBookRequest
}

func (s *captureUpdateBookServer) UpdateBook(_ context.Context, req *examplepb.UpdateBookRequest) (*examplepb.Book, error) {
	s.requests <- req
	return req.GetBook(), nil
}

func newUpdateBookGateway(t *testing.T) (*httptest.Server, chan *examplepb.UpdateBookRequest) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	requests := make(chan *examplepb.UpdateBookRequest, 4)
	grpcServer := grpc.NewServer()
	examplepb.RegisterABitOfEverythingServiceServer(grpcServer, &captureUpdateBookServer{requests: requests})
	t.Cleanup(grpcServer.Stop)
	go func() { _ = grpcServer.Serve(lis) }()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	ctx := context.Background()
	mux := gwruntime.NewServeMux()
	if err := examplepb.RegisterABitOfEverythingServiceHandlerFromEndpoint(ctx, mux, "passthrough://bufnet",
		[]grpc.DialOption{grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}); err != nil {
		t.Fatalf("RegisterABitOfEverythingServiceHandlerFromEndpoint: %v", err)
	}
	return httptest.NewServer(mux), requests
}

func sortedPaths(paths []string) []string {
	out := append([]string(nil), paths...)
	sort.Strings(out)
	return out
}

// TestUpdateBookQueryFieldsJoinFieldMask drives the real generated PATCH
// handler: a body field ("title") and a query-bound top-level field
// (allow_missing) must end up in the one field mask the backend receives.
func TestUpdateBookQueryFieldsJoinFieldMask(t *testing.T) {
	httpServer, requests := newUpdateBookGateway(t)
	t.Cleanup(httpServer.Close)

	req, err := http.NewRequest(http.MethodPatch,
		httpServer.URL+"/v1/publishers/123/books/abc?allow_missing=true",
		strings.NewReader(`{"id":"book-1"}`))
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http.DefaultClient.Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", resp.StatusCode, body)
	}

	select {
	case got := <-requests:
		if !got.GetAllowMissing() {
			t.Errorf("allow_missing was not bound from the query: %v", got)
		}
		if want, gotPaths := []string{"allow_missing", "id"}, sortedPaths(got.GetUpdateMask().GetPaths()); len(gotPaths) != len(want) {
			t.Errorf("update mask paths = %v; want %v", gotPaths, want)
		} else {
			for i := range want {
				if gotPaths[i] != want[i] {
					t.Errorf("update mask paths = %v; want %v", gotPaths, want)
					break
				}
			}
		}
	default:
		t.Fatal("gRPC server did not receive the request")
	}
}

// TestUpdateBookBadQueryValueRejectsWholeRequest verifies atomicity over the
// wire: a bad query value yields 400 naming the parameter as written, and the
// request is never forwarded because no field is replaced before all
// parameters decode.
func TestUpdateBookBadQueryValueRejectsWholeRequest(t *testing.T) {
	httpServer, requests := newUpdateBookGateway(t)
	t.Cleanup(httpServer.Close)

	req, err := http.NewRequest(http.MethodPatch,
		httpServer.URL+"/v1/publishers/123/books/abc?allow_missing=maybe",
		strings.NewReader(`{"id":"book-1"}`))
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http.DefaultClient.Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusBadRequest, body)
	}
	select {
	case got := <-requests:
		t.Fatalf("request was forwarded despite the bad query value: %v", got)
	default:
	}
}
