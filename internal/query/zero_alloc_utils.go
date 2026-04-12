package query

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/23skdu/longbow/internal/core"
)

func parseSearchByIDRequest(data []byte, req *core.VectorSearchByIDRequest) error {
	return json.Unmarshal(data, req)
}

func parseRecommendRequest(data []byte, req *core.RecommendRequest) error {
	return json.Unmarshal(data, req)
}

func skipWhitespace(data []byte, pos int) int {
	for pos < len(data) && (data[pos] == ' ' || data[pos] == '\t' || data[pos] == '\n' || data[pos] == '\r') {
		pos++
	}
	return pos
}

func parseString(data []byte, pos int) (string, int, error) {
	if pos >= len(data) || data[pos] != '"' {
		return "", pos, errors.New("expected quote")
	}
	pos++
	start := pos
	hasEscapes := false
	for pos < len(data) && data[pos] != '"' {
		if data[pos] == '\\' {
			hasEscapes = true
			pos += 2
			continue
		}
		pos++
	}
	if pos >= len(data) {
		return "", pos, errors.New("unexpected end of string")
	}
	s := string(data[start:pos])
	if hasEscapes {
		s = string(decodeEscapes([]byte(s)))
	}
	return s, pos + 1, nil
}

func decodeEscapes(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	res := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		if data[i] == '\\' && i+1 < len(data) {
			i++
			switch data[i] {
			case '"':
				res = append(res, '"')
			case '\\':
				res = append(res, '\\')
			case '/':
				res = append(res, '/')
			case 'b':
				res = append(res, '\b')
			case 'f':
				res = append(res, '\f')
			case 'n':
				res = append(res, '\n')
			case 'r':
				res = append(res, '\r')
			case 't':
				res = append(res, '\t')
			case 'u':
				if i+4 < len(data) {
					u, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 64)
					if err == nil {
						buf := make([]byte, 4)
						n := encodeRune(buf, rune(u))
						res = append(res, buf[:n]...)
						i += 4
					} else {
						res = append(res, 'u')
					}
				} else {
					res = append(res, 'u')
				}
			default:
				res = append(res, data[i])
			}
		} else {
			res = append(res, data[i])
		}
	}
	return res
}

func encodeRune(buf []byte, r rune) int {
	if r <= 0x7F {
		buf[0] = byte(r)
		return 1
	}
	if r <= 0x7FF {
		buf[0] = 0xC0 | byte(r>>6)
		buf[1] = 0x80 | byte(r&0x3F)
		return 2
	}
	if r <= 0xFFFF {
		buf[0] = 0xE0 | byte(r>>12)
		buf[1] = 0x80 | byte((r>>6)&0x3F)
		buf[2] = 0x80 | byte(r&0x3F)
		return 3
	}
	if r <= 0x10FFFF {
		buf[0] = 0xF0 | byte(r>>18)
		buf[1] = 0x80 | byte((r>>12)&0x3F)
		buf[2] = 0x80 | byte((r>>6)&0x3F)
		buf[3] = 0x80 | byte(r&0x3F)
		return 4
	}
	return 0
}

func parseInt64(data []byte, pos int) (int64, int, error) {
	start := pos
	if pos < len(data) && data[pos] == '-' {
		pos++
	}
	if pos >= len(data) || (data[pos] < '0' || data[pos] > '9') {
		return 0, start, errors.New("expected digit")
	}
	for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
		pos++
	}
	numStr := string(data[start:pos])
	val, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, start, err
	}
	return val, pos, nil
}

func parseFloat32(data []byte, pos int) (float32, int, error) {
	start := pos
	if pos < len(data) && data[pos] == '-' {
		pos++
	}
	if pos >= len(data) || (data[pos] < '0' || data[pos] > '9') {
		return 0, start, errors.New("expected digit in number")
	}
	for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
		pos++
	}
	if pos < len(data) && data[pos] == '.' {
		pos++
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}
	if pos < len(data) && (data[pos] == 'e' || data[pos] == 'E') {
		pos++
		if pos < len(data) && (data[pos] == '+' || data[pos] == '-') {
			pos++
		}
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}
	val, err := strconv.ParseFloat(string(data[start:pos]), 32)
	if err != nil {
		return 0, start, err
	}
	return float32(val), pos, nil
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

func skipValue(data []byte, pos int) (int, error) {
	pos = skipWhitespace(data, pos)
	if pos >= len(data) {
		return pos, errors.New("unexpected end")
	}
	switch data[pos] {
	case '{':
		return skipObject(data, pos)
	case '[':
		return skipArray(data, pos)
	case '"':
		_, newPos, err := parseString(data, pos)
		return newPos, err
	case 't', 'f', 'n':
		return skipLiteral(data, pos)
	default:
		return skipNumber(data, pos)
	}
}

func skipLiteral(data []byte, pos int) (int, error) {
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "true" {
		return pos + 4, nil
	}
	if pos+5 <= len(data) && string(data[pos:pos+5]) == "false" {
		return pos + 5, nil
	}
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return pos + 4, nil
	}
	return pos, errors.New("expected literal")
}

func skipNumber(data []byte, pos int) (int, error) {
	start := pos
	if pos < len(data) && data[pos] == '-' {
		pos++
	}
	if pos >= len(data) || (data[pos] < '0' || data[pos] > '9') {
		return start, errors.New("expected number")
	}
	for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
		pos++
	}
	if pos < len(data) && data[pos] == '.' {
		pos++
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}
	if pos < len(data) && (data[pos] == 'e' || data[pos] == 'E') {
		pos++
		if pos < len(data) && (data[pos] == '+' || data[pos] == '-') {
			pos++
		}
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}
	return pos, nil
}

func skipObject(data []byte, pos int) (int, error) {
	pos++ // {
	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, errors.New("unclosed object")
		}
		if data[pos] == '}' {
			return pos + 1, nil
		}
		var err error
		pos, err = skipValue(data, pos)
		if err != nil {
			return pos, err
		}
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return pos, errors.New("expected colon")
		}
		pos++
		pos, err = skipValue(data, pos)
		if err != nil {
			return pos, err
		}
		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, errors.New("unclosed object")
}

func skipArray(data []byte, pos int) (int, error) {
	pos++ // [
	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, errors.New("unclosed array")
		}
		if data[pos] == ']' {
			return pos + 1, nil
		}
		var err error
		pos, err = skipValue(data, pos)
		if err != nil {
			return pos, err
		}
		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, errors.New("unclosed array")
}

func safeString(b []byte) string {
	return string(b)
}

func parseFilter(data []byte, pos int) (core.Filter, int, error) {
	var f core.Filter
	if pos >= len(data) || data[pos] != '{' {
		return f, pos, errors.New("expected {")
	}
	pos++
	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == '}' {
			if pos < len(data) {
				pos++
			}
			return f, pos, nil
		}
		key, newPos, err := parseString(data, pos)
		if err != nil {
			return f, pos, err
		}
		pos = newPos
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return f, pos, errors.New("expected colon")
		}
		pos++
		pos = skipWhitespace(data, pos)
		switch key {
		case "field":
			val, newPos, err := parseString(data, pos)
			if err != nil {
				return f, pos, err
			}
			f.Field = val
			pos = newPos
		case "operator", "op":
			val, newPos, err := parseString(data, pos)
			if err != nil {
				return f, pos, err
			}
			f.Operator = val
			pos = newPos
		case "value":
			if data[pos] == '"' {
				val, newPos, err := parseString(data, pos)
				if err != nil {
					return f, pos, err
				}
				f.Value = val
				pos = newPos
			} else {
				start := pos
				for pos < len(data) && data[pos] != ',' && data[pos] != '}' && data[pos] != ']' && data[pos] != ' ' && data[pos] != '\t' && data[pos] != '\n' && data[pos] != '\r' {
					pos++
				}
				f.Value = safeString(data[start:pos])
			}
		case "logic":
			val, newPos, err := parseString(data, pos)
			if err != nil {
				return f, pos, err
			}
			f.Logic = val
			pos = newPos
		case "filters":
			var sub []core.Filter
			newPos, err := parseFilterArray(data, pos, &sub)
			if err != nil {
				return f, pos, err
			}
			f.Filters = sub
			pos = newPos
		default:
			pos, err = skipValue(data, pos)
			if err != nil {
				return f, pos, err
			}
		}
		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return f, pos, nil
}

func parseFilterArray(data []byte, pos int, filters *[]core.Filter) (int, error) {
	if pos >= len(data) || data[pos] != '[' {
		return pos, errors.New("expected [")
	}
	pos++
	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == ']' {
			if pos < len(data) {
				pos++
			}
			return pos, nil
		}
		f, newPos, err := parseFilter(data, pos)
		if err != nil {
			return pos, err
		}
		*filters = append(*filters, f)
		pos = newPos
		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, nil
}

func parseWindowFunctionsShared(data []byte, pos int) ([]core.WindowFunction, int, error) {
	if pos >= len(data) || data[pos] != '[' {
		return nil, pos, errors.New("expected [")
	}
	pos++
	var wfs []core.WindowFunction
	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == ']' {
			if pos < len(data) {
				pos++
			}
			return wfs, pos, nil
		}
		wf, newPos, err := parseWindowFunctionShared(data, pos)
		if err != nil {
			return nil, pos, err
		}
		wfs = append(wfs, wf)
		pos = newPos
		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return nil, pos, errors.New("unexpected end")
}

func parseWindowFunctionShared(data []byte, pos int) (core.WindowFunction, int, error) {
	var wf core.WindowFunction
	if pos >= len(data) || data[pos] != '{' {
		return wf, pos, errors.New("expected {")
	}
	pos++
	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == '}' {
			if pos < len(data) {
				pos++
			}
			return wf, pos, nil
		}
		key, newPos, err := parseString(data, pos)
		if err != nil {
			return wf, pos, err
		}
		pos = newPos
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return wf, pos, errors.New("expected :")
		}
		pos++
		pos = skipWhitespace(data, pos)
		switch key {
		case "name":
			val, newPos, err := parseString(data, pos)
			if err != nil {
				return wf, pos, err
			}
			wf.Name = val
			pos = newPos
		case "as":
			val, newPos, err := parseString(data, pos)
			if err != nil {
				return wf, pos, err
			}
			wf.As = val
			pos = newPos
		case "field":
			val, newPos, err := parseString(data, pos)
			if err != nil {
				return wf, pos, err
			}
			wf.Field = val
			pos = newPos
		case "over":
			val, newPos, err := parseWindowSpecShared(data, pos)
			if err != nil {
				return wf, pos, err
			}
			wf.Over = val
			pos = newPos
		default:
			pos, err = skipValue(data, pos)
			if err != nil {
				return wf, pos, err
			}
		}
		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return wf, pos, nil
}

func parseWindowSpecShared(data []byte, pos int) (core.WindowSpec, int, error) {
	var spec core.WindowSpec
	if pos >= len(data) || data[pos] != '{' {
		return spec, pos, errors.New("expected {")
	}
	pos++
	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == '}' {
			if pos < len(data) {
				pos++
			}
			return spec, pos, nil
		}
		key, newPos, err := parseString(data, pos)
		if err != nil {
			return spec, pos, err
		}
		pos = newPos
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return spec, pos, errors.New("expected :")
		}
		pos++
		pos = skipWhitespace(data, pos)
		switch key {
		case "partition_by":
			if data[pos] != '[' {
				return spec, pos, errors.New("expected [")
			}
			pos++
			for pos < len(data) {
				pos = skipWhitespace(data, pos)
				if data[pos] == ']' {
					pos++
					break
				}
				val, newPos, err := parseString(data, pos)
				if err != nil {
					return spec, pos, err
				}
				spec.PartitionBy = append(spec.PartitionBy, val)
				pos = newPos
				pos = skipWhitespace(data, pos)
				if pos < len(data) && data[pos] == ',' {
					pos++
				}
			}
		case "order_by":
			if data[pos] != '[' {
				return spec, pos, errors.New("expected [")
			}
			pos++
			for pos < len(data) {
				pos = skipWhitespace(data, pos)
				if data[pos] == ']' {
					pos++
					break
				}
				val, newPos, err := parseWindowOrderShared(data, pos)
				if err != nil {
					return spec, pos, err
				}
				spec.OrderBy = append(spec.OrderBy, val)
				pos = newPos
				pos = skipWhitespace(data, pos)
				if pos < len(data) && data[pos] == ',' {
					pos++
				}
			}
		default:
			pos, err = skipValue(data, pos)
			if err != nil {
				return spec, pos, err
			}
		}
		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return spec, pos, nil
}

func parseWindowOrderShared(data []byte, pos int) (core.WindowOrder, int, error) {
	var order core.WindowOrder
	if pos >= len(data) || data[pos] != '{' {
		return order, pos, errors.New("expected {")
	}
	pos++
	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == '}' {
			if pos < len(data) {
				pos++
			}
			return order, pos, nil
		}
		key, newPos, err := parseString(data, pos)
		if err != nil {
			return order, pos, err
		}
		pos = newPos
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return order, pos, errors.New("expected :")
		}
		pos++
		pos = skipWhitespace(data, pos)
		switch key {
		case "field":
			val, newPos, err := parseString(data, pos)
			if err != nil {
				return order, pos, err
			}
			order.Field = val
			pos = newPos
		case "desc", "descending":
			val, newPos, err := parseBool(data, pos)
			if err != nil {
				return order, pos, err
			}
			order.Descending = val
			pos = newPos
		default:
			pos, err = skipValue(data, pos)
			if err != nil {
				return order, pos, err
			}
		}
		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return order, pos, nil
}
