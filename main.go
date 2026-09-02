package main

import (
	"forgejo.develop.10.199.64.20.nip.io/zergx/worker-go/internal/env"
)

var (
	workdir = env.Or("WORKER_WORKSPACE_ROOT", "/root/workspace")
	port    = env.Or("WORKER_PORT", "48080")
)
