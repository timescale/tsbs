package zigtsdb

import (
	"fmt"
	"testing"
	"time"
)

var (
	testFatalCount int
	testLastFatal  string
)

// captureFatal swaps the package fatal hook so tests can assert on it.
func captureFatal(t *testing.T) func() {
	t.Helper()
	prev := fatal
	testFatalCount = 0
	testLastFatal = ""
	fatal = func(format string, args ...interface{}) {
		testFatalCount++
		testLastFatal = fmt.Sprintf(format, args...)
	}
	return func() { fatal = prev }
}

func fatalCalls() int   { return testFatalCount }
func lastFatal() string { return testLastFatal }

func stubSleep(fn func(time.Duration)) func() {
	prev := sleep
	sleep = fn
	return func() { sleep = prev }
}

func newTestFactory() *factory {
	return newFactory(1 << 20)
}
