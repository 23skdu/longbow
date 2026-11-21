package query

import (
	"errors"
	"strconv"
)

func SkipWhitespace(data []byte, pos int) int {
	for pos < len(data) && (data[pos] == ' ' || data[pos] == '\t' || data[pos] == '\n' || data[pos] == '\r') {
		pos++
	}
	return pos
}

func ParseString(data []byte, pos int) (string, int, error) {
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
						n := encodeRune(buf, rune(u)) // #nosec G115
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
		buf[0] = byte(r) // #nosec G115
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

func ParseInt64(data []byte, pos int) (int64, int, error) {
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

func ParseFloat32(data []byte, pos int) (float32, int, error) {
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

func ParseBool(data []byte, pos int) (bool, int, error) {
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "true" {
		return true, pos + 4, nil
	}
	if pos+5 <= len(data) && string(data[pos:pos+5]) == "false" {
		return false, pos + 5, nil
	}
	return false, pos, errors.New("expected boolean")
}

func SkipValue(data []byte, pos int) (int, error) {
	pos = SkipWhitespace(data, pos)
	if pos >= len(data) {
		return pos, errors.New("unexpected end")
	}
	switch data[pos] {
	case '{':
		return SkipObject(data, pos)
	case '[':
		return SkipArray(data, pos)
	case '"':
		_, newPos, err := ParseString(data, pos)
		return newPos, err
	case 't', 'f', 'n':
		return SkipLiteral(data, pos)
	default:
		return SkipNumber(data, pos)
	}
}

func SkipLiteral(data []byte, pos int) (int, error) {
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

func SkipNumber(data []byte, pos int) (int, error) {
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

func SkipObject(data []byte, pos int) (int, error) {
	pos++ // {
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, errors.New("unclosed object")
		}
		if data[pos] == '}' {
			return pos + 1, nil
		}
		var err error
		pos, err = SkipValue(data, pos)
		if err != nil {
			return pos, err
		}
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return pos, errors.New("expected colon")
		}
		pos++
		pos, err = SkipValue(data, pos)
		if err != nil {
			return pos, err
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, errors.New("unclosed object")
}

func SkipArray(data []byte, pos int) (int, error) {
	pos++ // [
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, errors.New("unclosed array")
		}
		if data[pos] == ']' {
			return pos + 1, nil
		}
		var err error
		pos, err = SkipValue(data, pos)
		if err != nil {
			return pos, err
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, errors.New("unclosed array")
}

func safeString(b []byte) string {
	return string(b)
}
