package query

import (
	"errors"
	"time"

	"github.com/23skdu/longbow/internal/core"
)

func ParseTemporalSearchRequest(data []byte, req *core.TemporalSearchRequest) error {
	i := SkipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = SkipWhitespace(data, i)
		if i >= len(data) {
			return errors.New("unexpected end of JSON")
		}
		if data[i] == '}' {
			return nil
		}
		key, newPos, err := ParseString(data, i)
		if err != nil {
			return err
		}
		i = newPos
		i = SkipWhitespace(data, i)
		if i >= len(data) || data[i] != ':' {
			return errors.New("expected colon")
		}
		i++
		i = SkipWhitespace(data, i)

		switch key {
		case "dataset":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.Dataset = val
			i = newPos
		case "search_type":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.SearchType = val
			i = newPos
		case "k":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.K = int(val)
			i = newPos
		case "timestamp":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.Timestamp = val
			i = newPos
		case "start_time":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.StartTime = val
			i = newPos
		case "end_time":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.EndTime = val
			i = newPos
		case "window_size":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.WindowSize = int(val)
			i = newPos
		case "duration":
			if i < len(data) && data[i] == '"' {
				s, newPos, err := ParseString(data, i)
				if err != nil {
					return err
				}
				d, derr := time.ParseDuration(s)
				if derr != nil {
					return derr
				}
				req.Duration = d
				i = newPos
			} else {
				val, newPos, err := ParseInt64(data, i)
				if err != nil {
					return err
				}
				req.Duration = time.Duration(val)
				i = newPos
			}
		case "filters":
			newPos, err := ParseFilterArray(data, i, &req.Filters)
			if err != nil {
				return err
			}
			i = newPos
		default:
			i, err = SkipValue(data, i)
			if err != nil {
				return err
			}
		}

		i = SkipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
	return nil
}

// ParseRangeSearchRequest parses a JSON object into RangeSearchRequest.
func ParseRangeSearchRequest(data []byte, req *core.RangeSearchRequest) error {
	i := SkipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = SkipWhitespace(data, i)
		if i >= len(data) {
			return errors.New("unexpected end of JSON")
		}
		if data[i] == '}' {
			return nil
		}
		key, newPos, err := ParseString(data, i)
		if err != nil {
			return err
		}
		i = SkipWhitespace(data, newPos)
		if i >= len(data) || data[i] != ':' {
			return errors.New("expected colon")
		}
		i = SkipWhitespace(data, i+1)

		switch key {
		case "dataset":
			val, next, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.Dataset = val
			i = next
		case "vector":
			vec, next, err := parseFloat32Slice(data, i)
			if err != nil {
				return err
			}
			req.Vector = vec
			i = next
		case "threshold":
			val, next, err := ParseFloat32(data, i)
			if err != nil {
				return err
			}
			req.Threshold = val
			i = next
		case "max_results":
			val, next, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.MaxResults = int(val)
			i = next
		case "filters":
			next, err := ParseFilterArray(data, i, &req.Filters)
			if err != nil {
				return err
			}
			i = next
		default:
			i, err = SkipValue(data, i)
			if err != nil {
				return err
			}
		}

		i = SkipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
	return errors.New("unexpected end of JSON")
}

// ParseFederatedSearchRequest parses a JSON object into FederatedSearchRequest.
func ParseFederatedSearchRequest(data []byte, req *core.FederatedSearchRequest) error {
	i := SkipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = SkipWhitespace(data, i)
		if i >= len(data) {
			return errors.New("unexpected end of JSON")
		}
		if data[i] == '}' {
			return nil
		}
		key, newPos, err := ParseString(data, i)
		if err != nil {
			return err
		}
		i = SkipWhitespace(data, newPos)
		if i >= len(data) || data[i] != ':' {
			return errors.New("expected colon")
		}
		i = SkipWhitespace(data, i+1)

		switch key {
		case "collections":
			cols, next, err := parseStringSlice(data, i)
			if err != nil {
				return err
			}
			req.Collections = cols
			i = next
		case "tags":
			tags, next, err := parseStringSlice(data, i)
			if err != nil {
				return err
			}
			req.Tags = tags
			i = next
		case "vector":
			vec, next, err := parseFloat32Slice(data, i)
			if err != nil {
				return err
			}
			req.Vector = vec
			i = next
		case "k":
			val, next, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.K = int(val)
			i = next
		case "rrf_k":
			val, next, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.RRFK = int(val)
			i = next
		default:
			i, err = SkipValue(data, i)
			if err != nil {
				return err
			}
		}

		i = SkipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
	return errors.New("unexpected end of JSON")
}

func parseFloat32Slice(data []byte, pos int) ([]float32, int, error) {
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return nil, pos + 4, nil
	}
	if pos >= len(data) || data[pos] != '[' {
		return nil, pos, errors.New("expected opening bracket for float array")
	}
	pos++
	var out []float32
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) {
			return nil, pos, errors.New("unexpected end of float array")
		}
		if data[pos] == ']' {
			return out, pos + 1, nil
		}
		v, next, err := ParseFloat32(data, pos)
		if err != nil {
			return nil, pos, err
		}
		out = append(out, v)
		pos = SkipWhitespace(data, next)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return nil, pos, errors.New("unexpected end of float array")
}

func parseStringSlice(data []byte, pos int) ([]string, int, error) {
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return nil, pos + 4, nil
	}
	if pos >= len(data) || data[pos] != '[' {
		return nil, pos, errors.New("expected opening bracket for string array")
	}
	pos++
	var out []string
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) {
			return nil, pos, errors.New("unexpected end of string array")
		}
		if data[pos] == ']' {
			return out, pos + 1, nil
		}
		v, next, err := ParseString(data, pos)
		if err != nil {
			return nil, pos, err
		}
		out = append(out, v)
		pos = SkipWhitespace(data, next)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return nil, pos, errors.New("unexpected end of string array")
}
