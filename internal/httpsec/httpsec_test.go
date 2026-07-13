package httpsec

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func reqWithOrigin(origin, host string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://"+host+"/ws", nil)
	r.Host = host
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	return r
}

func TestOriginChecker(t *testing.T) {
	tests := []struct {
		name    string
		allowed []string
		origin  string
		host    string
		want    bool
	}{
		{"wildcard allows any", []string{"*"}, "https://evil.example", "localhost:8080", true},
		{"no origin header accepted", nil, "", "localhost:8080", true},
		{"exact match", []string{"https://app.example"}, "https://app.example", "app.example", true},
		{"exact match trailing slash", []string{"https://app.example/"}, "https://app.example", "app.example", true},
		{"not in allowlist rejected", []string{"https://app.example"}, "https://evil.example", "app.example", false},
		{"empty allowlist loopback ok", nil, "http://localhost:3000", "localhost:8080", true},
		{"empty allowlist 127.0.0.1 ok", nil, "http://127.0.0.1:3000", "localhost:8080", true},
		{"empty allowlist same host ok", nil, "http://myhost", "myhost", true},
		{"empty allowlist cross host rejected", nil, "http://evil.example", "myhost", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := OriginChecker(tt.allowed)
			got := check(reqWithOrigin(tt.origin, tt.host))
			if got != tt.want {
				t.Fatalf("OriginChecker(%v) origin=%q host=%q = %v, want %v",
					tt.allowed, tt.origin, tt.host, got, tt.want)
			}
		})
	}
}

func TestTokenValid(t *testing.T) {
	t.Run("empty expected disables auth", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/api/execute", nil)
		if !TokenValid(r, "") {
			t.Fatal("expected auth disabled when expected token empty")
		}
	})

	t.Run("bearer header", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/api/execute", nil)
		r.Header.Set("Authorization", "Bearer secret")
		if !TokenValid(r, "secret") {
			t.Fatal("valid bearer token rejected")
		}
	})

	t.Run("x-auth-token header", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/api/execute", nil)
		r.Header.Set("X-Auth-Token", "secret")
		if !TokenValid(r, "secret") {
			t.Fatal("valid X-Auth-Token rejected")
		}
	})

	t.Run("query param", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/api/execute?token=secret", nil)
		if !TokenValid(r, "secret") {
			t.Fatal("valid query token rejected")
		}
	})

	t.Run("missing token rejected", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/api/execute", nil)
		if TokenValid(r, "secret") {
			t.Fatal("missing token accepted")
		}
	})

	t.Run("wrong token rejected", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/api/execute", nil)
		r.Header.Set("Authorization", "Bearer nope")
		if TokenValid(r, "secret") {
			t.Fatal("wrong token accepted")
		}
	})
}

func TestRequireToken(t *testing.T) {
	called := false
	next := func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}

	t.Run("rejects unauthenticated", func(t *testing.T) {
		called = false
		h := RequireToken("secret", next)
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/api/execute", nil))
		if called {
			t.Fatal("next called for unauthenticated request")
		}
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("allows authenticated", func(t *testing.T) {
		called = false
		h := RequireToken("secret", next)
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/execute", nil)
		r.Header.Set("Authorization", "Bearer secret")
		h(rec, r)
		if !called {
			t.Fatal("next not called for authenticated request")
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
	})
}
