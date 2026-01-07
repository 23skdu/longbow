package query

import (
	"errors"
	"math"
	"strconv"
)

// ZeroAllocVectorSearchParser parses VectorSearchRequest JSON with minimal allocations.
// The parser pre-allocates a vector slice and reuses it across parse calls.
// For the common case (no escape sequences in dataset name), this achieves
// zero allocations beyond the initial pre-allocation.
type ZeroAllocVectorSearchParser struct {
	result  VectorSearchRequest
	vector  []float32 // pre-allocated vector buffer
	filters []Filter  // pre-allocated filters buffer
}

// NewZeroAllocVectorSearchParser creates a new reusable parser.
// maxDims specifies the maximum expected vector dimensions for pre-allocation.
func NewZeroAllocVectorSearchParser(maxDims int) *ZeroAllocVectorSearchParser {
	return &ZeroAllocVectorSearchParser{
		vector:  make([]float32, 0, maxDims),
		filters: make([]Filter, 0, 16),
	}
}

// Parse parses the JSON data into a VectorSearchRequest.
// The returned VectorSearchRequest.Vector shares the parser's internal buffer,
// so the result is only valid until the next Parse call.
func (p *ZeroAllocVectorSearchParser) Parse(data []byte) (VectorSearchRequest, error) {
	// Reset state
	p.result.Dataset = ""
	p.result.K = 0
	p.result.TextQuery = ""
	p.result.Alpha = 0
	p.result.GraphAlpha = 0 // Reset
	p.vector = p.vector[:0]
	p.filters = p.filters[:0]

	if len(data) == 0 {
		return p.result, nil
	}

	i := skipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return p.result, errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = skipWhitespace(data, i)
		if i >= len(data) {
			return p.result, errors.New("unexpected end of JSON")
		}

		if data[i] == '}' {
			// Copy vector to result (shares backing array)
			p.result.Vector = p.vector
			if len(p.filters) > 0 {
				p.result.Filters = make([]Filter, len(p.filters))
				copy(p.result.Filters, p.filters)
			} else {
				p.result.Filters = nil
			}
			return p.result, nil
		}

		if data[i] != '"' {
			return p.result, errors.New("expected quote for key")
		}

		key, newPos, err := parseString(data, i)
		if err != nil {
			return p.result, err
		}
		i = newPos

		i = skipWhitespace(data, i)
		if i >= len(data) || data[i] != ':' {
			return p.result, errors.New("expected colon")
		}
		i++
		i = skipWhitespace(data, i)

		switch key {
		case "dataset":
			val, newPos, err := parseString(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.Dataset = val
			i = newPos
		case "k":
			val, newPos, err := parseInt64(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.K = int(val)
			i = newPos
		case "vector":
			newPos, err := p.parseFloat32Array(data, i)
			if err != nil {
				return p.result, err
			}
			i = newPos
		case "filters":
			newPos, err := p.parseFilters(data, i)
			if err != nil {
				return p.result, err
			}
			i = newPos
		case "local_only":
			val, newPos, err := parseBool(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.LocalOnly = val
			i = newPos
		case "text_query":
			val, newPos, err := parseString(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.TextQuery = val
			i = newPos
		case "alpha":
			val, newPos, err := parseFloat32(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.Alpha = val
			i = newPos
		case "graph_alpha":
			val, newPos, err := parseFloat32(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.GraphAlpha = val
			i = newPos
		default:
			// Unknown field: return error to trigger fallback to json.Unmarshal
			// This is important because the zero-alloc parser doesn't support 'vectors' yet.
			return p.result, errors.New("unknown field: " + key)
		}

		i = skipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}

	return p.result, errors.New("unexpected end of JSON")
}

// parseFloat32Array parses a JSON array of numbers into p.vector
func (p *ZeroAllocVectorSearchParser) parseFloat32Array(data []byte, pos int) (int, error) {
	if pos >= len(data) || data[pos] != '[' {
		return pos, errors.New("expected opening bracket for vector")
	}
	pos++

	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, errors.New("unexpected end in vector array")
		}

		if data[pos] == ']' {
			return pos + 1, nil
		}

		// Parse float value
		val, newPos, err := parseFloat32(data, pos)
		if err != nil {
			return pos, err
		}
		p.vector = append(p.vector, val)
		pos = newPos

		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
			// Check for trailing comma
			next := skipWhitespace(data, pos)
			if next < len(data) && data[next] == ']' {
				return pos, errors.New("trailing comma in vector array")
			}
		}
	}

	return pos, errors.New("unexpected end in vector array")
}

// parseFloat32 parses a JSON number as float32.
// Handles integers, decimals, and scientific notation.
func parseFloat32(data []byte, pos int) (float32, int, error) { //nolint:gocritic // unnamedResult - clarity preferred for parser utility
	start := pos

	// Handle negative sign
	if pos < len(data) && data[pos] == '-' {
		pos++
	}

	// Integer part
	if pos >= len(data) || (data[pos] < '0' || data[pos] > '9') {
		return 0, start, errors.New("expected digit in number")
	}
	for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
		pos++
	}

	// Fractional part
	if pos < len(data) && data[pos] == '.' {
		pos++
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}

	// Exponent part
	if pos < len(data) && (data[pos] == 'e' || data[pos] == 'E') {
		pos++
		if pos < len(data) && (data[pos] == '+' || data[pos] == '-') {
			pos++
		}
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}

	// Use strconv.ParseFloat for accurate conversion
	val, err := strconv.ParseFloat(string(data[start:pos]), 32)
	if err != nil {
		return 0, start, err
	}

	return float32(val), pos, nil
}

func (p *ZeroAllocVectorSearchParser) parseFilters(data []byte, pos int) (int, error) {
	if pos >= len(data) || data[pos] != '[' {
		return pos, errors.New("expected opening bracket")
	}
	pos++

	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, errors.New("unexpected end in filters")
		}

		if data[pos] == ']' {
			return pos + 1, nil
		}

		f, newPos, err := parseFilter(data, pos)
		if err != nil {
			return pos, err
		}
		p.filters = append(p.filters, f)
		pos = newPos

		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}

	return pos, errors.New("unexpected end in filters")
}

func parseBool(data []byte, pos int) (bool, int, error) {
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "true" {
		return true, pos + 4, nil
	}
	if pos+5 <= len(data) && string(data[pos:pos+5]) == "false" {
		return false, pos + 5, nil
	}
	return false, pos, errors.New("expected boolean")
}

// Ensure imports are used
var _ = math.MaxFloat32
