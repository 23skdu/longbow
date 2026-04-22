package query

import (
	"errors"

	"github.com/rs/zerolog"
)

// ZeroAllocVectorSearchParser parses VectorSearchRequest JSON with minimal allocations.
// The parser pre-allocates a vector slice and reuses it across parse calls.
// For the common case (no escape sequences in dataset name), this achieves
// zero allocations beyond the initial pre-allocation.
type ZeroAllocVectorSearchParser struct {
	result  VectorSearchRequest
	vector  []float32 // pre-allocated vector buffer
	filters []Filter  // pre-allocated filters buffer
	logger  zerolog.Logger
}

// NewZeroAllocVectorSearchParser creates a new reusable parser.
// maxDims specifies the maximum expected vector dimensions for pre-allocation.
func NewZeroAllocVectorSearchParser(maxDims int, logger *zerolog.Logger) *ZeroAllocVectorSearchParser {
	return &ZeroAllocVectorSearchParser{
		vector:  make([]float32, 0, maxDims),
		filters: make([]Filter, 0, 16),
		logger:  *logger,
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
	p.result.GraphAlpha = 0
	p.result.GraphDepth = 0
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
			// Copy vector to result (avoid sharing internal buffer after Parse returns)
			if len(p.vector) > 0 {
				p.result.Vector = make([]float32, len(p.vector))
				copy(p.result.Vector, p.vector)
			} else {
				p.result.Vector = nil
			}
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
			if i+4 <= len(data) && string(data[i:i+4]) == "null" {
				i += 4
			} else {
				newPos, err := p.ParseVectorField(data, i, &p.vector)
				if err != nil {
					return p.result, err
				}
				i = newPos
			}
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
			// Handle null
			if i+4 <= len(data) && string(data[i:i+4]) == "null" {
				p.result.TextQuery = ""
				i += 4
			} else {
				val, newPos, err := parseString(data, i)
				if err != nil {
					return p.result, err
				}
				p.result.TextQuery = val
				i = newPos
			}
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
		case "graph_depth":
			val, newPos, err := parseInt64(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.GraphDepth = int(val)
			i = newPos
		case "include_vectors":
			val, newPos, err := parseBool(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.IncludeVectors = val
			i = newPos
		case "vector_format":
			// Handle null
			if i+4 <= len(data) && string(data[i:i+4]) == "null" {
				p.result.VectorFormat = ""
				i += 4
			} else {
				val, newPos, err := parseString(data, i)
				if err != nil {
					return p.result, err
				}
				p.result.VectorFormat = val
				i = newPos
			}
		case "window_functions":
			wfs, newPos, err := parseWindowFunctionsShared(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.WindowFunctions = wfs
			i = newPos
		default:
			// Unknown field: return error to trigger fallback to json.Unmarshal
			// This is important because the zero-alloc parser doesn't support 'vectors' yet.
			p.logger.Warn().Str("key", key).Msg("DEBUG: Unknown key in VectorSearchRequest")
			return p.result, errors.New("unknown field: " + key)
		}

		i = skipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}

	return p.result, errors.New("unexpected end of JSON")
}

// ParseVectorField parses a JSON array of numbers into the provided slice
func (p *ZeroAllocVectorSearchParser) ParseVectorField(data []byte, pos int, outVec *[]float32) (int, error) {
	if pos >= len(data) || data[pos] != '[' {
		return pos, errors.New("expected opening bracket for vector")
	}
	pos++

	// Clear outVec but keep capacity if any
	*outVec = (*outVec)[:0]

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
		*outVec = append(*outVec, val)
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


func (p *ZeroAllocVectorSearchParser) parseFilters(data []byte, pos int) (int, error) {
	return parseFilterArray(data, pos, &p.filters)
}
