package query

import (
	"errors"
	"strconv"

	"github.com/23skdu/longbow/internal/core"
)

func ParseDatasetRequest(data []byte, dsName *string) error {
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

		if key == "dataset" || key == "name" {
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			*dsName = val
			i = newPos
		} else {
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

func ParseSearchByIDRequest(data []byte, req *core.VectorSearchByIDRequest) error {
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
		if data[i] != '"' {
			return errors.New("expected quote for key")
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
		case "id":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.ID = val
			i = newPos
		case "k":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.K = int(val)
			i = newPos
		case "include_vectors":
			val, newPos, err := ParseBool(data, i)
			if err != nil {
				return err
			}
			req.IncludeVectors = val
			i = newPos
		case "vector_format":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.VectorFormat = val
			i = newPos
		case "vector_type":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.VectorType = val
			i = newPos
		case "turboquant_bits":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.TurboQuantBits = int(val)
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

func ParseRecommendRequest(data []byte, req *core.RecommendRequest) error {
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
		case "seed_ids":
			if i < len(data) && data[i] == '[' {
				i++
				for i < len(data) {
					i = SkipWhitespace(data, i)
					if data[i] == ']' {
						i++
						break
					}
					val, newPos, err := ParseString(data, i)
					if err != nil {
						return err
					}
					req.SeedIDs = append(req.SeedIDs, val)
					i = newPos
					i = SkipWhitespace(data, i)
					if i < len(data) && data[i] == ',' {
						i++
					}
				}
			} else {
				i, _ = SkipValue(data, i)
			}
		case "k":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.K = int(val)
			i = newPos
		case "alpha":
			val, newPos, err := ParseFloat32(data, i)
			if err != nil {
				return err
			}
			req.Alpha = val
			i = newPos
		case "max_hops":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.MaxHops = int(val)
			i = newPos
		case "decay":
			val, newPos, err := ParseFloat32(data, i)
			if err != nil {
				return err
			}
			req.Decay = val
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

func ParseGeoSearchRequest(data []byte, req *core.GeoSearchRequest) error {
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
		case "center":
			newPos, err := ParseGeoPoint(data, i, &req.Center)
			if err != nil {
				return err
			}
			i = newPos
		case "radius_km":
			val, newPos, err := ParseFloat32(data, i)
			if err != nil {
				return err
			}
			req.RadiusKm = float64(val)
			i = newPos
		case "box":
			if req.Box == nil {
				req.Box = &core.GeoBoundingBox{}
			}
			newPos, err := ParseGeoBoundingBox(data, i, req.Box)
			if err != nil {
				return err
			}
			i = newPos
		case "k":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.K = int(val)
			i = newPos
		case "search_type":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.SearchType = val
			i = newPos
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

func ParseGeoPoint(data []byte, pos int, point *core.GeoPoint) (int, error) {
	pos = SkipWhitespace(data, pos)
	if pos >= len(data) || data[pos] != '{' {
		return pos, errors.New("expected { for geopoint")
	}
	pos++
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if data[pos] == '}' {
			return pos + 1, nil
		}
		key, newPos, err := ParseString(data, pos)
		if err != nil {
			return pos, err
		}
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if data[pos] != ':' {
			return pos, errors.New("expected :")
		}
		pos++
		pos = SkipWhitespace(data, pos)
		switch key {
		case "lat":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			point.Lat = float64(val)
			pos = newPos
		case "lon":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			point.Lon = float64(val)
			pos = newPos
		case "name":
			val, newPos, err := ParseString(data, pos)
			if err != nil {
				return pos, err
			}
			point.Name = val
			pos = newPos
		default:
			pos, err = SkipValue(data, pos)
			if err != nil {
				return pos, err
			}
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, nil
}

func ParseGeoBoundingBox(data []byte, pos int, box *core.GeoBoundingBox) (int, error) {
	pos = SkipWhitespace(data, pos)
	if pos >= len(data) || data[pos] != '{' {
		return pos, errors.New("expected { for geobox")
	}
	pos++
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if data[pos] == '}' {
			return pos + 1, nil
		}
		key, newPos, err := ParseString(data, pos)
		if err != nil {
			return pos, err
		}
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if data[pos] != ':' {
			return pos, errors.New("expected :")
		}
		pos++
		pos = SkipWhitespace(data, pos)
		switch key {
		case "min_lat":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			box.MinLat = float64(val)
			pos = newPos
		case "max_lat":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			box.MaxLat = float64(val)
			pos = newPos
		case "min_lon":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			box.MinLon = float64(val)
			pos = newPos
		case "max_lon":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			box.MaxLon = float64(val)
			pos = newPos
		default:
			pos, err = SkipValue(data, pos)
			if err != nil {
				return pos, err
			}
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, nil
}

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



func ParseFilterRecursive(data []byte, pos int, parser *ZeroAllocTicketParser) (core.Filter, int, error) {
	var f core.Filter
	if pos >= len(data) || data[pos] != '{' {
		return f, pos, errors.New("expected {")
	}
	pos++
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == '}' {
			if pos < len(data) {
				pos++
			}
			return f, pos, nil
		}
		key, newPos, err := ParseString(data, pos)
		if err != nil {
			return f, pos, err
		}
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return f, pos, errors.New("expected colon")
		}
		pos++
		pos = SkipWhitespace(data, pos)
		switch key {
		case "field":
			val, newPos, err := ParseString(data, pos)
			if err != nil {
				return f, pos, err
			}
			f.Field = val
			pos = newPos
		case "operator", "op":
			val, newPos, err := ParseString(data, pos)
			if err != nil {
				return f, pos, err
			}
			f.Operator = val
			pos = newPos
		case "value":
			if data[pos] == '"' {
				val, newPos, err := ParseString(data, pos)
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
			val, newPos, err := ParseString(data, pos)
			if err != nil {
				return f, pos, err
			}
			f.Logic = val
			pos = newPos
		case "filters":
			var sub []core.Filter
			newPos, err := ParseFilterArrayRecursive(data, pos, &sub, parser)
			if err != nil {
				return f, pos, err
			}
			f.Filters = sub
			pos = newPos
		case "subquery":
			if parser != nil {
				start := pos
				newPos, err := SkipObject(data, pos)
				if err != nil {
					return f, pos, err
				}
				// We need a fresh parser or reset state for subquery to avoid corrupting current result
				subParser := NewZeroAllocTicketParser(&parser.logger)
				subQuery, err := subParser.Parse(data[start:newPos])
				if err != nil {
					return f, pos, err
				}
				f.Subquery = &subQuery
				pos = newPos
			} else {
				// If no parser context, skip
				pos, err = SkipValue(data, pos)
				if err != nil {
					return f, pos, err
				}
			}
		default:
			pos, err = SkipValue(data, pos)
			if err != nil {
				return f, pos, err
			}
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return f, pos, nil
}

func ParseFilterArray(data []byte, pos int, filters *[]core.Filter) (int, error) {
	return ParseFilterArrayRecursive(data, pos, filters, nil)
}

func ParseFilterArrayRecursive(data []byte, pos int, filters *[]core.Filter, parser *ZeroAllocTicketParser) (int, error) {
	pos = SkipWhitespace(data, pos)
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return pos + 4, nil
	}
	if pos >= len(data) || data[pos] != '[' {
		return pos, errors.New("expected [")
	}
	pos++
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == ']' {
			if pos < len(data) {
				pos++
			}
			return pos, nil
		}
		f, newPos, err := ParseFilterRecursive(data, pos, parser)
		if err != nil {
			return pos, err
		}
		*filters = append(*filters, f)
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, nil
}

func ParseWindowFunctionsShared(data []byte, pos int) ([]core.WindowFunction, int, error) {
	pos = SkipWhitespace(data, pos)
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return nil, pos + 4, nil
	}
	if pos >= len(data) || data[pos] != '[' {
		return nil, pos, errors.New("expected [")
	}
	pos++
	var wfs []core.WindowFunction
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == ']' {
			if pos < len(data) {
				pos++
			}
			return wfs, pos, nil
		}
		wf, newPos, err := ParseWindowFunctionShared(data, pos)
		if err != nil {
			return nil, pos, err
		}
		wfs = append(wfs, wf)
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return nil, pos, errors.New("unexpected end")
}

func ParseWindowFunctionShared(data []byte, pos int) (core.WindowFunction, int, error) {
	var wf core.WindowFunction
	if pos >= len(data) || data[pos] != '{' {
		return wf, pos, errors.New("expected {")
	}
	pos++
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == '}' {
			if pos < len(data) {
				pos++
			}
			return wf, pos, nil
		}
		key, newPos, err := ParseString(data, pos)
		if err != nil {
			return wf, pos, err
		}
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return wf, pos, errors.New("expected :")
		}
		pos++
		pos = SkipWhitespace(data, pos)
		switch key {
		case "name":
			val, newPos, err := ParseString(data, pos)
			if err != nil {
				return wf, pos, err
			}
			wf.Name = val
			pos = newPos
		case "as":
			val, newPos, err := ParseString(data, pos)
			if err != nil {
				return wf, pos, err
			}
			wf.As = val
			pos = newPos
		case "field":
			val, newPos, err := ParseString(data, pos)
			if err != nil {
				return wf, pos, err
			}
			wf.Field = val
			pos = newPos
		case "over":
			val, newPos, err := ParseWindowSpecShared(data, pos)
			if err != nil {
				return wf, pos, err
			}
			wf.Over = val
			pos = newPos
		default:
			pos, err = SkipValue(data, pos)
			if err != nil {
				return wf, pos, err
			}
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return wf, pos, nil
}

func ParseWindowSpecShared(data []byte, pos int) (core.WindowSpec, int, error) {
	var spec core.WindowSpec
	if pos >= len(data) || data[pos] != '{' {
		return spec, pos, errors.New("expected {")
	}
	pos++
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == '}' {
			if pos < len(data) {
				pos++
			}
			return spec, pos, nil
		}
		key, newPos, err := ParseString(data, pos)
		if err != nil {
			return spec, pos, err
		}
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return spec, pos, errors.New("expected :")
		}
		pos++
		pos = SkipWhitespace(data, pos)
		switch key {
		case "partition_by":
			pos = SkipWhitespace(data, pos)
			if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
				pos += 4
				break
			}
			if data[pos] != '[' {
				return spec, pos, errors.New("expected [")
			}
			pos++
			for pos < len(data) {
				pos = SkipWhitespace(data, pos)
				if data[pos] == ']' {
					pos++
					break
				}
				val, newPos, err := ParseString(data, pos)
				if err != nil {
					return spec, pos, err
				}
				spec.PartitionBy = append(spec.PartitionBy, val)
				pos = newPos
				pos = SkipWhitespace(data, pos)
				if pos < len(data) && data[pos] == ',' {
					pos++
				}
			}
		case "order_by":
			pos = SkipWhitespace(data, pos)
			if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
				pos += 4
				break
			}
			if data[pos] != '[' {
				return spec, pos, errors.New("expected [")
			}
			pos++
			for pos < len(data) {
				pos = SkipWhitespace(data, pos)
				if data[pos] == ']' {
					pos++
					break
				}
				val, newPos, err := ParseWindowOrderShared(data, pos)
				if err != nil {
					return spec, pos, err
				}
				spec.OrderBy = append(spec.OrderBy, val)
				pos = newPos
				pos = SkipWhitespace(data, pos)
				if pos < len(data) && data[pos] == ',' {
					pos++
				}
			}
		default:
			pos, err = SkipValue(data, pos)
			if err != nil {
				return spec, pos, err
			}
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return spec, pos, nil
}

func ParseWindowOrderShared(data []byte, pos int) (core.WindowOrder, int, error) {
	var order core.WindowOrder
	if pos >= len(data) || data[pos] != '{' {
		return order, pos, errors.New("expected {")
	}
	pos++
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] == '}' {
			if pos < len(data) {
				pos++
			}
			return order, pos, nil
		}
		key, newPos, err := ParseString(data, pos)
		if err != nil {
			return order, pos, err
		}
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return order, pos, errors.New("expected :")
		}
		pos++
		pos = SkipWhitespace(data, pos)
		switch key {
		case "field":
			val, newPos, err := ParseString(data, pos)
			if err != nil {
				return order, pos, err
			}
			order.Field = val
			pos = newPos
		case "desc", "descending":
			val, newPos, err := ParseBool(data, pos)
			if err != nil {
				return order, pos, err
			}
			order.Descending = val
			pos = newPos
		default:
			pos, err = SkipValue(data, pos)
			if err != nil {
				return order, pos, err
			}
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return order, pos, nil
}
