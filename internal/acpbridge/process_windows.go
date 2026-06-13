//go:build windows

package acpbridge

import "os/exec"

func prepareChildProcess(cmd *exec.Cmd) {}
