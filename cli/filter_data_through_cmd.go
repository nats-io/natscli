package cli

import (
	"bytes"
	"fmt"
	"github.com/google/shlex"
	"os/exec"
	"strings"
	"text/template"
)

func outPutMSGBody(data []byte, filter, subject, stream string) {
	if len(data) == 0 {
		fmt.Println("nil body")
		return
	}

	data, err := filterDataThroughCmd(data, filter, subject, stream)
	if err != nil {
		// using q here so raw binary data will be escaped
		fmt.Printf("%q\nError while translating msg body: %s\n\n", data, err.Error())
		return
	}
	output := string(data)
	fmt.Println(output)
	if !strings.HasSuffix(output, "\n") {
		fmt.Println()
	}
}

func filterDataThroughCmd(data []byte, filter, subject, stream string) ([]byte, error) {
	if filter == "" {
		return data, nil
	}
	funcMap := template.FuncMap{
		"Subject": func() string { return subject },
		"Stream":  func() string { return stream },
	}

	tmpl, err := template.New("translate").Funcs(funcMap).Parse(filter)
	if err != nil {
		return nil, err
	}
	var builder strings.Builder
	err = tmpl.Execute(&builder, nil)
	if err != nil {
		return nil, err
	}

	parts, err := shlex.Split(builder.String())
	if err != nil {
		return nil, fmt.Errorf("the filter command line could not be parsed: %w", err)
	}
	cmd := parts[0]
	args := parts[1:]

	runner := exec.Command(cmd, args...)
	// pass the message as string to stdin
	runner.Stdin = bytes.NewReader(data)
	// maybe we want to do something on error?
	return runner.CombinedOutput()
}
