//go:build !windows

package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

func runCommand(cmd string, input string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execution := exec.Command(cmd, args...)

	execution.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if input != "" {
		execution.Stdin = strings.NewReader(input)
	}

	type result struct {
		out []byte
		err error
	}

	resCh := make(chan result, 1)

	go func() {
		out, err := execution.CombinedOutput()
		resCh <- result{out: out, err: err}
	}()

	select {
	case <-ctx.Done():
		if execution.Process != nil {
			_ = syscall.Kill(-execution.Process.Pid, syscall.SIGKILL)
		}
		return nil, fmt.Errorf("nats utility timed out")
	case res := <-resCh:
		if res.err != nil {
			return nil, fmt.Errorf("nats utility failed: %v\n%v", res.err, string(res.out))
		}
		return res.out, nil
	}
}
