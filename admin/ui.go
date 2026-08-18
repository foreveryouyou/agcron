package admin

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed index.html
var uiFiles embed.FS

// ui serves the embedded single-page task management UI. The UI talks to the
// same /api/* endpoints exposed by the API, so a browser hitting the admin
// root can create, edit, pause/resume, run and delete jobs at runtime.
func ui() http.Handler {
	sub, err := fs.Sub(uiFiles, ".")
	if err != nil {
		panic(err) // cannot happen: index.html is embedded at the package root
	}
	return http.FileServer(http.FS(sub))
}
