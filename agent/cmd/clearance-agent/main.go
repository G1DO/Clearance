package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	agent "clearance-agent"
)

func main() {
	controller := flag.String("controller", "http://localhost:8080", "controller HTTP(S) origin")
	stateDir := flag.String("state-dir", "", "durable directory dedicated to this runner (required)")
	cgroupRoot := flag.String("cgroup-root", "/sys/fs/cgroup/clearance", "delegated cgroup v2 ancestor containing the agent process")
	workspaceRoot := flag.String("workspace-root", "", "allocation workspace root (default state-dir plus -workspaces)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	daemon, err := agent.NewDaemon(agent.Config{ControllerURL: *controller, StateDir: *stateDir, MachineToken: os.Getenv("CLEARANCE_MACHINE_TOKEN"), CgroupRoot: *cgroupRoot, WorkspaceRoot: *workspaceRoot})
	if err == nil {
		err = daemon.Run(ctx)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
