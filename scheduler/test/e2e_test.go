package test

import (
	_ "net/http/pprof"
	"testing"
)

func TestE2E(t *testing.T) {
	RunE2ETests(t)
}
