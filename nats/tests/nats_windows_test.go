//go:build windows

package main

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func runCommand(cmd string, input string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execution := exec.Command(cmd, args...)
	execution.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}

	if input != "" {
		execution.Stdin = strings.NewReader(input)
	}

	outCh := make(chan []byte)
	errCh := make(chan error)
	go func() {
		out, err := execution.CombinedOutput()
		if err != nil {
			errCh <- err
		} else {
			outCh <- out
		}
	}()
	select {
	case <-ctx.Done():
		if execution.Process != nil {
			killCmd := exec.Command("cmd", "/c", "taskkill", "/F", "/T", "/PID", strconv.Itoa(execution.Process.Pid))
			_ = killCmd.Run()
		}
		return nil, fmt.Errorf("nats utility timed out")
	case err := <-errCh:
		return nil, fmt.Errorf("nats utility failed: %v\n%v", err, string(<-outCh))
	case out := <-outCh:
		return out, nil
	}
}
