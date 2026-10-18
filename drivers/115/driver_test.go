package _115

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"golang.org/x/time/rate"
)

func TestLimitRateAppliesToEveryHTTPRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := &Pan115{
		client:  driver115.New(),
		limiter: rate.NewLimiter(rate.Limit(2), 1),
	}
	d.applyRateLimit()

	for i := 0; i < 2; i++ {
		started := time.Now()
		resp, err := d.client.Client.R().Get(server.URL)
		if err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
		if resp.StatusCode() != http.StatusOK {
			t.Fatalf("request %d status=%d", i+1, resp.StatusCode())
		}
		if i == 1 && time.Since(started) < 450*time.Millisecond {
			t.Fatalf("request %d was not delayed by the configured 2 requests/second limit", i+1)
		}
	}
}
