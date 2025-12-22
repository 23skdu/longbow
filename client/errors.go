package client

import (
	"fmt"
	"regexp"
	"strings"
)

// ErrForwardRequired is returned when a request needs to be routed to another node
type ErrForwardRequired struct {
	TargetNodeID string
	TargetAddr   string
}

func (e *ErrForwardRequired) Error() string {
	return fmt.Sprintf("FORWARD_REQUIRED: target=%s addr=%s", e.TargetNodeID, e.TargetAddr)
}

// Regex to parse the error string
var forwardErrorRegex = regexp.MustCompile(`FORWARD_REQUIRED: target=(.*?) addr=(.*)`)

// IsForwardRequired checks if an error is a "FORWARD_REQUIRED" error
// If success, returns the parsed error struct, otherwise nil
func IsForwardRequired(err error) *ErrForwardRequired {
	if err == nil {
		return nil
	}

	// Check if it's already our type (unlikely if coming from gRPC wire)
	if e, ok := err.(*ErrForwardRequired); ok {
		return e
	}

	// Parse string error from gRPC status
	errMsg := err.Error()
	if !strings.Contains(errMsg, "FORWARD_REQUIRED") {
		return nil
	}

	// Use regex to extract fields
	matches := forwardErrorRegex.FindStringSubmatch(errMsg)
	if len(matches) == 3 {
		return &ErrForwardRequired{
			TargetNodeID: strings.TrimSpace(matches[1]),
			TargetAddr:   strings.TrimSpace(matches[2]),
		}
	}

	return nil
}
