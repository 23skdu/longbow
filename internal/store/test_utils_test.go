package store

import (
	"io"

	"github.com/rs/zerolog"
)

func mockLogger() zerolog.Logger {
	return zerolog.New(io.Discard)
}
