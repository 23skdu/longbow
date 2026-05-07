package safe

import (
	"fmt"
	"math"
)

// Uint32ToInt32 converts a uint32 to an int32, returning an error if it would overflow.
func Uint32ToInt32(val uint32) (int32, error) {
	if val > math.MaxInt32 {
		return 0, fmt.Errorf("uint32 value %d exceeds math.MaxInt32", val)
	}
	return int32(val), nil
}

// IntToInt32 converts an int to an int32, returning an error if it would overflow.
func IntToInt32(val int) (int32, error) {
	if val > math.MaxInt32 || val < math.MinInt32 {
		return 0, fmt.Errorf("int value %d exceeds int32 range", val)
	}
	return int32(val), nil
}

// Uint32ToInt converts a uint32 to an int.
func Uint32ToInt(val uint32) int {
	return int(val)
}

// Int64ToUint32 converts an int64 to a uint32 with overflow check.
func Int64ToUint32(val int64) (uint32, error) {
	if val < 0 || val > math.MaxUint32 {
		return 0, fmt.Errorf("int64 value %d out of uint32 range", val)
	}
	return uint32(val), nil
}
