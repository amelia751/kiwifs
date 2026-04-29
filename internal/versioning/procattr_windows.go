//go:build windows

package versioning

import (
	"os/exec"
	"time"
)

// setProcAttr is a no-op on Windows where Setpgid is not available.
func setProcAttr(cmd *exec.Cmd) {}

// setCancelSignal on Windows uses WaitDelay for a grace period before
// the process is killed. Windows has no SIGTERM equivalent for console
// apps, so the default kill behaviour is acceptable.
func setCancelSignal(cmd *exec.Cmd) {
	cmd.WaitDelay = 5 * time.Second
}
