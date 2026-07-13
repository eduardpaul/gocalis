// Package httpsec provides small helpers for hardening the HTTP/WebSocket APIs:
// WebSocket Origin allow-listing and bearer-token authentication for control
// endpoints.
package httpsec

import (
	"crypto/subtle"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// OriginChecker returns a function suitable for websocket.Upgrader.CheckOrigin.
//
// Rules:
//   - allowed contains "*"            -> any origin is accepted.
//   - request has no Origin header    -> accepted (non-browser client; no CSRF surface).
//   - Origin is in the allowed list   -> accepted (exact, case-insensitive match).
//   - allowed is empty                -> accept only localhost origins and origins
//     whose host matches the request Host (same-origin).
//   - otherwise                       -> rejected.
func OriginChecker(allowed []string) func(*http.Request) bool {
	allowAny := false
	set := make(map[string]struct{}, len(allowed))
	for _, o := range allowed {
		o = strings.TrimSpace(o)
		if o == "*" {
			allowAny = true
			continue
		}
		if o != "" {
			set[strings.ToLower(strings.TrimRight(o, "/"))] = struct{}{}
		}
	}

	return func(r *http.Request) bool {
		if allowAny {
			return true
		}

		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}

		key := strings.ToLower(strings.TrimRight(origin, "/"))
		if _, ok := set[key]; ok {
			return true
		}

		u, err := url.Parse(origin)
		if err != nil {
			return false
		}
		host := u.Hostname()

		if len(set) == 0 {
			if isLoopback(host) {
				return true
			}
			// Same-origin: Origin host matches the request's Host header.
			reqHost := r.Host
			if h, _, err := net.SplitHostPort(reqHost); err == nil {
				reqHost = h
			}
			return strings.EqualFold(host, reqHost)
		}

		return false
	}
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// Token extracts a bearer token from a request, checking (in order) the
// "Authorization: Bearer <token>" header, the "X-Auth-Token" header, and the
// "token" query parameter.
func Token(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			return strings.TrimSpace(auth[len("bearer "):])
		}
	}
	if t := r.Header.Get("X-Auth-Token"); t != "" {
		return t
	}
	return r.URL.Query().Get("token")
}

// TokenValid reports whether the request carries the expected token. When the
// expected token is empty, authentication is disabled and every request is valid.
func TokenValid(r *http.Request, expected string) bool {
	if expected == "" {
		return true
	}
	got := Token(r)
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

// RequireToken wraps a handler so it only runs when the request is authenticated.
// When expected is empty, authentication is disabled and next runs unconditionally.
func RequireToken(expected string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !TokenValid(r, expected) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
