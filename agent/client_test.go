package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientAuthenticationContractAndErrors(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer machine-key" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("missing POST machine authentication or JSON content type")
		}
		if r.URL.Path == "/internal/v1/agents/poll" {
			var body map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if len(body) != 2 || string(body["agent_incarnation"]) != "7" || string(body["timeout_s"]) != "10" {
				t.Errorf("poll body: %s", body)
			}
			fmt.Fprint(w, `{"assigned":false,"future":true,"recoveryGeneration":999}`)
			return
		}
		w.WriteHeader(418)
		fmt.Fprint(w, `{"error":"future_error","message":"future explanation","unknown":true}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "machine-key")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if p, err := client.Poll(context.Background(), 7, 10); err != nil || p.Assigned {
		t.Fatalf("poll: %+v %v", p, err)
	}
	report := ReportRequest{AllocationID: "11111111-1111-1111-1111-111111111111", RunnerEpoch: 1, AgentIncarnation: 7, Seq: 1, Status: StatusHeartbeat, Ts: time.Now()}
	_, err = client.Report(context.Background(), report)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 418 || httpErr.Body.Error != "future_error" || retryable(err) {
		t.Fatalf("unknown error: %v", err)
	}
	if requests.Load() != 2 {
		t.Fatalf("unexpected retries: %d", requests.Load())
	}
}

func TestClientBoundsCancellationAndRedirects(t *testing.T) {
	for _, scenario := range []string{"redirect", "oversize", "invalid", "drop", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			var redirected atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
			defer target.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				switch scenario {
				case "redirect":
					http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
				case "oversize":
					fmt.Fprint(w, strings.Repeat(" ", maxResponseBytes+1))
				case "invalid":
					fmt.Fprint(w, `{"assigned":true}`)
				case "drop":
					w.WriteHeader(503)
				case "cancel":
					<-r.Context().Done()
				}
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "secret")
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, err = client.Poll(ctx, 1, 10)
			if err == nil {
				t.Fatal("accepted invalid reply")
			}
			if scenario == "drop" && !retryable(err) {
				t.Fatalf("empty 503 not retryable: %v", err)
			}
			if scenario == "cancel" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline: %v", err)
			}
			if redirected.Load() != 0 {
				t.Fatal("forwarded machine identity through redirect")
			}
		})
	}
}

func TestClientInvalidConfiguration(t *testing.T) {
	for _, origin := range []string{"", "ftp://localhost", "http://user:password@localhost", "http://localhost/path", "http://localhost?q=1", "http://localhost?", "http://localhost/#fragment"} {
		if _, err := NewClient(origin, "key"); err == nil {
			t.Errorf("accepted %q", origin)
		}
	}
	for _, key := range []string{"", "key\nInjected: value"} {
		if _, err := NewClient("http://localhost", key); err == nil {
			t.Error("accepted invalid machine key")
		}
	}
}
