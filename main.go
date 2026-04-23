package main

import (
	"github.com/kiwifs/kiwifs/cmd"
)

// Version is set via ldflags during build
var version = "dev"

func init() {
	cmd.Version = version
}

func main() {
	cmd.Execute()
}
