//go:build !windows

package versioning

import (
	"os/exec"
	"syscall"
	"time"
)

// setProcAttr puts the child process in its own process group so that
// exec.CommandContext's kill signal propagates to the entire group
// (ssh, git-lfs, credential helpers) instead of leaving orphans.
func setProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// setCancelSignal configures the command to send SIGTERM on context
// cancellation instead of the default SIGKILL. This gives git a chance
// to release index.lock before dying. If the process doesn't exit
// within 5 seconds of SIGTERM, WaitDelay triggers SIGKILL.
// This is the pattern used by Gitea and GitLab Gitaly.
func setCancelSignal(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	cmd.WaitDelay = 5 * time.Second
}
