package query

import (
	"errors"

	"github.com/23skdu/longbow/internal/core"
)

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
			return wf, pos, errors.New("expected colon")
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
			return spec, pos, errors.New("expected colon")
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
			return order, pos, errors.New("expected colon")
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
