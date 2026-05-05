package version

import (
	"fmt"
	"runtime"
)

var (
	// Version is the current version of Longbow
	Version = "dev"
	// Commit is the git commit hash
	Commit = "none"
	// BuildDate is the date when the binary was built
	BuildDate = "unknown"
)

// Info returns a formatted version string
func Info() string {
	return fmt.Sprintf("Longbow version %s\nCommit: %s\nBuildDate: %s\nGoVersion: %s\nOS/Arch: %s/%s",
		Version, Commit, BuildDate, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

// Print prints the version information to stdout
func Print() {
	fmt.Println(Info())
}
