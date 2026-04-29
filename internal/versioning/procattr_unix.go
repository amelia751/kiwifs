//go:build !windows

package versioning

import (
	"os/exec"
	"syscall"
)

// setProcAttr puts the child process in its own process group so that
// exec.CommandContext's kill signal propagates to the entire group
// (ssh, git-lfs, credential helpers) instead of leaving orphans.
func setProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
