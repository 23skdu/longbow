package main

import (
	"bytes"
	"strings"
	"testing"
)

func FuzzParseCLIFlags(f *testing.F) {
	seeds := []string{
		"--max-memory 1073741824",
		"--listen-addr 0.0.0.0:3000",
		"--data-path /tmp/data",
		"--node-id test-node",
		"--log-format json",
		"--log-level debug",
		"--gpu-enabled",
		"--gpu-device-id 0",
		"--gossip-enabled",
		"--version",
		"-v",
		"--help",
		"--max-memory=17179869184 --listen-addr=0.0.0.0:5000",
		"--log-format console --log-level error",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if input == "" {
			t.Skip("empty input")
		}

		args := strings.Fields(input)
		if len(args) == 0 {
			t.Skip("no args")
		}

		var buf bytes.Buffer
		cfg := Config{}
		err := parseCLIFlags(&cfg, args, &buf)

		if err != nil {
			if err == ErrVersionRequested || err == ErrHelpRequested {
				return
			}
			// Unknown flags and malformed values are expected to error
			if strings.Contains(err.Error(), "flag provided but not defined") {
				return
			}
			if strings.Contains(err.Error(), "invalid value") {
				return
			}
			t.Errorf("unexpected error for args %q: %v", input, err)
		}
	})
}
