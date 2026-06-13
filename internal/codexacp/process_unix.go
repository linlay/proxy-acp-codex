//go:build darwin || linux

package codexacp

import (
	"os/exec"
	"syscall"
)

func prepareChildProcess(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}
