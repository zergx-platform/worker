package main

import ()

var (
	workdir = envOr("WORKER_WORKSPACE_ROOT", "/root/workspace")
	port    = envOr("WORKER_PORT", "48080")
)
