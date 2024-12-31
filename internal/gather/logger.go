package gather

import (
	"fmt"
	"io"
	"log"

	"github.com/nats-io/jsm.go/api"
)

type logger struct {
	capture io.Writer
	lvl     api.Level
	logFunc func(format string, a ...any)
}

func NewLogger(capture io.Writer, level api.Level) api.Logger {
	l := &logger{
		lvl:     level,
		logFunc: log.Printf,
		capture: capture,
	}

	return l
}

func (l *logger) Tracef(format string, a ...any) {
	if l.lvl >= api.TraceLevel {
		l.logFunc(format, a...)

		// only capture these when enabled
		if l.capture != nil {
			fmt.Fprintf(l.capture, format+"\n", a...)
		}
	}
}

func (l *logger) Debugf(format string, a ...any) {
	if l.lvl >= api.DebugLevel {
		l.logFunc(format, a...)

		// only capture these when enabled
		if l.capture != nil {
			fmt.Fprintf(l.capture, format+"\n", a...)
		}
	}
}

func (l *logger) Infof(format string, a ...any) {
	if l.lvl >= api.InfoLevel {
		l.logFunc(format, a...)
	}

	// always capture these
	if l.capture != nil {
		fmt.Fprintf(l.capture, format+"\n", a...)
	}
}

func (l *logger) Errorf(format string, a ...any) {
	if l.lvl >= api.ErrorLevel {
		l.logFunc(format, a...)
	}

	// always capture these
	if l.capture != nil {
		fmt.Fprintf(l.capture, format+"\n", a...)
	}
}
