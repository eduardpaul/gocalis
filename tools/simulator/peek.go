package main

import (
	"bufio"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

type peekConn struct {
	net.Conn
	r *bufio.Reader
}

func newPeekConn(c net.Conn) *peekConn         { return &peekConn{c, bufio.NewReaderSize(c, 8192)} }
func (c *peekConn) Read(b []byte) (int, error) { return c.r.Read(b) }
func (c *peekConn) device() (string, error) {
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer c.SetReadDeadline(time.Time{})
	for n := 1; n <= 8192; n++ {
		b, e := c.r.Peek(n)
		if e != nil {
			return "", e
		}
		if b[n-1] == '\n' {
			parts := strings.Fields(string(b))
			if len(parts) < 2 {
				return "", fmt.Errorf("malformed RTSP")
			}
			u, e := url.Parse(parts[1])
			if e != nil {
				return "", e
			}
			return strings.Trim(u.Path, "/"), nil
		}
	}
	return "", fmt.Errorf("oversized RTSP request")
}
