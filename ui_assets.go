package main

import (
	"embed"
	"io/fs"
	"log"

	"github.com/kiwifs/kiwifs/internal/webui"
)

// The `all:` prefix pulls in files beginning with `.` and `_` too, which Vite
// never emits but is cheap insurance.
//
//go:embed all:ui/dist
var uiAssets embed.FS

func init() {
	sub, err := fs.Sub(uiAssets, "ui/dist")
	if err != nil {
		log.Printf("webui: fs.Sub failed: %v", err)
		return
	}
	webui.SetAssets(sub)
}
