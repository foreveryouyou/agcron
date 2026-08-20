package admin

import (
	"embed"
	"html/template"
	"net/http"
)

//go:embed index.html
var uiFiles embed.FS

// indexTpl renders the embedded single-page task management UI with the API
// URL prefix injected, so the browser always calls the correct /api/*
// endpoints no matter how the admin is mounted (with or without a trailing
// slash, at any prefix depth).
var indexTpl = template.Must(template.ParseFS(uiFiles, "index.html"))

// ui renders the task management UI. The API prefix comes from template data
// rather than the request path, so "/agcron", "/agcron/" and deeper mounts all
// behave identically — the bare prefix works without a redirect and without
// breaking the relative /api/* requests made by the page.
func (a *API) ui() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = indexTpl.Execute(w, map[string]string{"APIPrefix": a.prefix})
	})
}
