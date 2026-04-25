package query

import (
	"errors"
	"time"
)

// ZeroAllocTemporalParser parses TemporalSearchRequest and TemporalAggregationRequest JSON with minimal allocations.
type ZeroAllocTemporalParser struct {
}

func NewZeroAllocTemporalParser() *ZeroAllocTemporalParser {
	return &ZeroAllocTemporalParser{}
}

// ParseSearch parses the JSON data into a TemporalSearchRequest.
func (p *ZeroAllocTemporalParser) ParseSearch(data []byte) (TemporalSearchRequest, error) {
	var res TemporalSearchRequest
	if len(data) == 0 {
		return res, nil
	}

	i := SkipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return res, errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = SkipWhitespace(data, i)
		if i >= len(data) {
			return res, errors.New("unexpected end of JSON")
		}

		if data[i] == '}' {
			return res, nil
		}

		if data[i] != '"' {
			return res, errors.New("expected quote for key")
		}

		key, newPos, err := ParseString(data, i)
		if err != nil {
			return res, err
		}
		i = newPos

		i = SkipWhitespace(data, i)
		if i >= len(data) || data[i] != ':' {
			return res, errors.New("expected colon")
		}
		i++
		i = SkipWhitespace(data, i)

		switch key {
		case "search_type":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return res, err
			}
			res.SearchType = val
			i = newPos
		case "k":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return res, err
			}
			res.K = int(val)
			i = newPos
		case "timestamp":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return res, err
			}
			res.Timestamp = val
			i = newPos
		case "start_time":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return res, err
			}
			res.StartTime = val
			i = newPos
		case "end_time":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return res, err
			}
			res.EndTime = val
			i = newPos
		case "window_size":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return res, err
			}
			res.WindowSize = int(val)
			i = newPos
		case "duration":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return res, err
			}
			res.Duration = time.Duration(val)
			i = newPos
		default:
			// Skip unknown fields
			newPos, err := SkipValue(data, i)
			if err != nil {
				return res, err
			}
			i = newPos
		}

		i = SkipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}

	return res, errors.New("unexpected end of JSON")
}

// ParseAggregation parses the JSON data into a TemporalAggregationRequest.
func (p *ZeroAllocTemporalParser) ParseAggregation(data []byte) (TemporalAggregationRequest, error) {
	var res TemporalAggregationRequest
	if len(data) == 0 {
		return res, nil
	}

	i := SkipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return res, errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = SkipWhitespace(data, i)
		if i >= len(data) {
			return res, errors.New("unexpected end of JSON")
		}

		if data[i] == '}' {
			return res, nil
		}

		if data[i] != '"' {
			return res, errors.New("expected quote for key")
		}

		key, newPos, err := ParseString(data, i)
		if err != nil {
			return res, err
		}
		i = newPos

		i = SkipWhitespace(data, i)
		if i >= len(data) || data[i] != ':' {
			return res, errors.New("expected colon")
		}
		i++
		i = SkipWhitespace(data, i)

		switch key {
		case "aggregation_type":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return res, err
			}
			res.AggregationType = val
			i = newPos
		case "start_time":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return res, err
			}
			res.StartTime = val
			i = newPos
		case "end_time":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return res, err
			}
			res.EndTime = val
			i = newPos
		case "interval":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return res, err
			}
			res.Interval = val
			i = newPos
		case "metric_field":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return res, err
			}
			res.MetricField = val
			i = newPos
		default:
			// Skip unknown fields
			newPos, err := SkipValue(data, i)
			if err != nil {
				return res, err
			}
			i = newPos
		}

		i = SkipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}

	return res, errors.New("unexpected end of JSON")
}
