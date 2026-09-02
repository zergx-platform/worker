package main

import (
	"github.com/zergx-platform/worker/internal/env"
)

var (
	workdir = env.Or("WORKER_WORKSPACE_ROOT", "/root/workspace")
	port    = env.Or("WORKER_PORT", "48080")
)
