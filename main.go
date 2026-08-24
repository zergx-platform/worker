package main

import (
	"os"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	workdir = envOr("WORKER_WORKSPACE_ROOT", "/root/workspace")
	port    = envOr("WORKER_PORT", "8080")
)
