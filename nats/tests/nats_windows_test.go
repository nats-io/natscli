//go:build windows

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func runCommand(cmd string, input string, env map[string]string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execution := exec.Command(cmd, args...)
	execution.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}

	if input != "" {
		execution.Stdin = strings.NewReader(input)
	}

	if len(env) > 0 {
		execution.Env = os.Environ()
		for k, v := range env {
			execution.Env = append(execution.Env, fmt.Sprintf("%s=%s", k, v))
		}
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
			killCmd := exec.Command("cmd", "/c", "taskkill", "/F", "/T", "/PID", strconv.Itoa(execution.Process.Pid))
			_ = killCmd.Run()
		}
		return nil, fmt.Errorf("nats utility timed out")
	case res := <-resCh:
		if res.err != nil {
			return res.out, fmt.Errorf("nats utility failed: %v\n%v", res.err, string(res.out))
		}
		return res.out, nil
	}
}
