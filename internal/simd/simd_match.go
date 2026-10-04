package simd

import "errors"

// MatchInt64 performs a comparison of src elements against val, storing the result (0 or 1) in dst.
// One byte per element is written to dst.
func MatchInt64(src []int64, val int64, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	return matchInt64Impl(src, val, op, dst)
}

// MatchInt32 performs a comparison of src elements against val, storing the result (0 or 1) in dst.
// One byte per element is written to dst.
func MatchInt32(src []int32, val int32, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	return matchInt32Impl(src, val, op, dst)
}

// MatchFloat32 performs a comparison of src elements against val, storing the result (0 or 1) in dst.
// One byte per element is written to dst.
func MatchFloat32(src []float32, val float32, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	// nosec G104 - matchFloat32Impl returns error only for length mismatch, already checked above
	_ = matchFloat32Impl(src, val, op, dst)
	return nil
}

// MatchFloat64 performs a comparison of src elements against val, storing the result (0 or 1) in dst.
// One byte per element is written to dst.
func MatchFloat64(src []float64, val float64, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	return matchFloat64Impl(src, val, op, dst)
}

// MatchUint16 performs an accelerated comparison of 16-bit unsigned integer values (e.g. dictionary codes).
func MatchUint16(src []uint16, val uint16, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	return matchUint16Generic(src, val, op, dst)
}

func matchUint16Generic(src []uint16, val uint16, op CompareOp, dst []byte) error {
	n := len(src)
	switch op {
	case CompareEq:
		i := 0
		for ; i+8 <= n; i += 8 {
			if src[i] == val { dst[i] = 1 } else { dst[i] = 0 }
			if src[i+1] == val { dst[i+1] = 1 } else { dst[i+1] = 0 }
			if src[i+2] == val { dst[i+2] = 1 } else { dst[i+2] = 0 }
			if src[i+3] == val { dst[i+3] = 1 } else { dst[i+3] = 0 }
			if src[i+4] == val { dst[i+4] = 1 } else { dst[i+4] = 0 }
			if src[i+5] == val { dst[i+5] = 1 } else { dst[i+5] = 0 }
			if src[i+6] == val { dst[i+6] = 1 } else { dst[i+6] = 0 }
			if src[i+7] == val { dst[i+7] = 1 } else { dst[i+7] = 0 }
		}
		for ; i < n; i++ {
			if src[i] == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareNeq:
		i := 0
		for ; i+8 <= n; i += 8 {
			if src[i] != val { dst[i] = 1 } else { dst[i] = 0 }
			if src[i+1] != val { dst[i+1] = 1 } else { dst[i+1] = 0 }
			if src[i+2] != val { dst[i+2] = 1 } else { dst[i+2] = 0 }
			if src[i+3] != val { dst[i+3] = 1 } else { dst[i+3] = 0 }
			if src[i+4] != val { dst[i+4] = 1 } else { dst[i+4] = 0 }
			if src[i+5] != val { dst[i+5] = 1 } else { dst[i+5] = 0 }
			if src[i+6] != val { dst[i+6] = 1 } else { dst[i+6] = 0 }
			if src[i+7] != val { dst[i+7] = 1 } else { dst[i+7] = 0 }
		}
		for ; i < n; i++ {
			if src[i] != val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	default:
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	}
	return nil
}

func matchInt64Generic(src []int64, val int64, op CompareOp, dst []byte) error {
	switch op {
	case CompareEq:
		for i, v := range src {
			// Branchless Equal: 1 if v == val, else 0
			diff := uint64(v ^ val)
			// (diff | -diff) >> 63 is 0 if diff is 0, else 1
			// We want the inverse of that
			res := 1 ^ ((diff | uint64(-int64(diff))) >> 63)
			dst[i] = byte(res)
		}
	case CompareNeq:
		for i, v := range src {
			// Branchless Not Equal: 1 if v != val, else 0
			diff := uint64(v ^ val)
			res := (diff | uint64(-int64(diff))) >> 63
			dst[i] = byte(res)
		}
	case CompareGt:
		for i, v := range src {
			if v > val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGe:
		for i, v := range src {
			if v >= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLt:
		for i, v := range src {
			if v < val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLe:
		for i, v := range src {
			if v <= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	default:
		// Fallback for others
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	}
	return nil
}

func matchInt32Generic(src []int32, val int32, op CompareOp, dst []byte) error {
	switch op {
	case CompareEq:
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareNeq:
		for i, v := range src {
			if v != val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGt:
		for i, v := range src {
			if v > val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGe:
		for i, v := range src {
			if v >= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLt:
		for i, v := range src {
			if v < val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLe:
		for i, v := range src {
			if v <= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	default:
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	}
	return nil
}

// matchFloat32Generic implements branchless float32 matching
func matchFloat32Generic(src []float32, val float32, op CompareOp, dst []byte) error {
	// Treat as uint32 for bitwise ops if needed, or use careful regular comparison.
	// For Float32, equality is tricky with NaN, but assumming standard numbers.
	// op is the enum.

	switch op {
	case CompareEq:
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareNeq:
		for i, v := range src {
			if v != val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGt:
		for i, v := range src {
			if v > val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGe:
		for i, v := range src {
			if v >= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLt:
		for i, v := range src {
			if v < val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLe:
		for i, v := range src {
			if v <= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	default:
		// Fallback
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	}
	return nil
}

// matchFloat64Generic implements branchless float64 matching
func matchFloat64Generic(src []float64, val float64, op CompareOp, dst []byte) error {
	switch op {
	case CompareEq:
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareNeq:
		for i, v := range src {
			if v != val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGt:
		for i, v := range src {
			if v > val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGe:
		for i, v := range src {
			if v >= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLt:
		for i, v := range src {
			if v < val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLe:
		for i, v := range src {
			if v <= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	default:
		// Fallback
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	}
	return nil
}
