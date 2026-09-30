package main

import (
	"embed"
	"net/http"
)

//go:embed ui/*
var assets embed.FS

func serveUI(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "/" {
		path = "/index.html"
	}
	b, e := assets.ReadFile("ui" + path)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	if path == "/index.html" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	}
	w.Write(b)
}
