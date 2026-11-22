package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestReportFatalError(t *testing.T) {
	var out bytes.Buffer
	reportFatalError(&out, errors.New("RBAC is always enforced"))

	got := out.String()
	if !strings.Contains(got, "Longbow exited with error: RBAC is always enforced") {
		t.Fatalf("reportFatalError() output = %q", got)
	}
	if !strings.HasSuffix(got, "\n") {
		t.Fatalf("reportFatalError() output is not newline-terminated: %q", got)
	}
}
