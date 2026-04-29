//go:build windows

package versioning

import "os/exec"

// setProcAttr is a no-op on Windows where Setpgid is not available.
// exec.CommandContext still terminates the child process on timeout;
// Windows process groups are managed differently by the OS.
func setProcAttr(cmd *exec.Cmd) {}
