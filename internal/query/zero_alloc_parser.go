package query

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
)

type TicketQuery struct {
	Name    string               `json:"name"`
	Limit   int64                `json:"limit"`
	Filters []Filter             `json:"filters"`
	Search  *VectorSearchRequest `json:"search,omitempty"`
}

type Filter struct {
	Field    string `json:"field,omitempty"`
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
	// Logic combines multiple filters: "AND", "OR", "NOT"
	Logic   string   `json:"logic,omitempty"`
	Filters []Filter `json:"filters,omitempty"`
}

// ZeroAllocTicketParser parses TicketQuery JSON with zero allocations
// for the common case (no escape sequences).
type ZeroAllocTicketParser struct {
	result       TicketQuery
	filters      []Filter
	searchParser *ZeroAllocVectorSearchParser
}

// NewZeroAllocTicketParser creates a new reusable parser
func NewZeroAllocTicketParser() *ZeroAllocTicketParser {
	return &ZeroAllocTicketParser{
		filters:      make([]Filter, 0, 16),
		searchParser: NewZeroAllocVectorSearchParser(768), // Default max dims
	}
}

// Parse parses the JSON data into a TicketQuery
func (p *ZeroAllocTicketParser) Parse(data []byte) (TicketQuery, error) {
	p.result.Name = ""
	p.result.Limit = 0
	p.result.Search = nil
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
		case "name":
			val, newPos, err := parseString(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.Name = val
			i = newPos
		case "dataset": // Alias for name
			val, newPos, err := parseString(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.Name = val
			i = newPos
		case "limit":
			val, newPos, err := parseInt64(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.Limit = val
			i = newPos
		case "filters":
			newPos, err := p.parseFilters(data, i)
			if err != nil {
				return p.result, err
			}
			i = newPos
		case "search":
			// Extract object slice
			start := i
			newPos, err := skipObject(data, i)
			if err != nil {
				return p.result, err
			}
			// Parse nested
			searchReq, err := p.searchParser.Parse(data[start:newPos])
			if err != nil {
				return p.result, err
			}
			p.result.Search = &searchReq
			i = newPos
		default:
			newPos, err := skipValue(data, i)
			if err != nil {
				return p.result, err
			}
			i = newPos
		}

		i = skipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}

	return p.result, errors.New("unexpected end of JSON")
}

func (p *ZeroAllocTicketParser) parseFilters(data []byte, pos int) (int, error) {
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return pos + 4, nil
	}
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

func parseFilter(data []byte, pos int) (Filter, int, error) {
	var f Filter

	if pos >= len(data) || data[pos] != '{' {
		return f, pos, errors.New("expected opening brace for filter")
	}
	pos++

	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return f, pos, errors.New("unexpected end in filter")
		}

		if data[pos] == '}' {
			return f, pos + 1, nil
		}

		if data[pos] != '"' {
			return f, pos, errors.New("expected quote for filter key")
		}

		key, newPos, err := parseString(data, pos)
		if err != nil {
			return f, pos, err
		}
		pos = newPos

		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return f, pos, errors.New("expected colon in filter")
		}
		pos++
		pos = skipWhitespace(data, pos)

		switch key {
		case "field":
			fieldVal, newPos, err := parseString(data, pos)
			if err != nil {
				return f, pos, err
			}
			f.Field = fieldVal
			pos = newPos
		case "operator", "op":
			opVal, newPos, err := parseString(data, pos)
			if err != nil {
				return f, pos, err
			}
			f.Operator = opVal
			pos = newPos
		case "value":
			// Value can be string or number (but stored as string)
			if pos < len(data) && data[pos] == '"' {
				strVal, newPos, err := parseString(data, pos)
				if err != nil {
					return f, pos, err
				}
				f.Value = strVal
				pos = newPos
			} else {
				// Numeric value - extract as string
				start := pos
				if pos < len(data) && data[pos] == '-' {
					pos++
				}
				for pos < len(data) && ((data[pos] >= '0' && data[pos] <= '9') || data[pos] == '.' || data[pos] == 'e' || data[pos] == 'E') {
					pos++
				}
				f.Value = safeString(data[start:pos])
			}
		case "logic":
			logicVal, newPos, err := parseString(data, pos)
			if err != nil {
				return f, pos, err
			}
			f.Logic = logicVal
			pos = newPos
		case "filters":
			// Recursive parsing of filters list
			// We need a temporary zeroParser-like logic or extract a list
			// Reusing ZeroAllocTicketParser.parseFilters logic but detached?
			// The existing parseFilters method is attached to ZeroAllocTicketParser and appends to p.filters.
			// Currently, TicketParser flattens top-level filters.
			// For recursive filters, we should support nested parsing.
			// Let's implement a standalone parseFilterList helper or reuse parseFilters by creating a new context?
			// Simpler: Just implement array parsing here loop.
			newFilters := make([]Filter, 0, 4)
			newPos, err := parseFilterArray(data, pos, &newFilters)
			if err != nil {
				return f, pos, err
			}
			f.Filters = newFilters
			pos = newPos
		default:
			newPos, err := skipValue(data, pos)
			if err != nil {
				return f, pos, err
			}
			pos = newPos
		}

		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}

	return f, pos, errors.New("unexpected end in filter")
}

func parseFilterArray(data []byte, pos int, filters *[]Filter) (int, error) {
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return pos + 4, nil
	}
	if pos >= len(data) || data[pos] != '[' {
		return pos, errors.New("expected opening bracket for filters")
	}
	pos++

	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, errors.New("unexpected end in nested filters")
		}

		if data[pos] == ']' {
			return pos + 1, nil
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

	return pos, errors.New("unexpected end in nested filters")
}

// ... Primitives ...

func skipWhitespace(data []byte, pos int) int {
	for pos < len(data) && (data[pos] == ' ' || data[pos] == '\t' || data[pos] == '\n' || data[pos] == '\r') {
		pos++
	}
	return pos
}

func safeString(b []byte) string {
	return string(b)
}

func parseString(data []byte, pos int) (s string, newPos int, err error) {
	if pos >= len(data) || data[pos] != '"' {
		return "", pos, errors.New("expected quote at start of string")
	}
	pos++
	start := pos

	// Fast path: scan for end quote, check if escapes present
	hasEscape := false
	for i := pos; i < len(data); i++ {
		c := data[i]
		if c == '"' {
			if !hasEscape {
				// No escapes, return direct slice (zero-alloc)
				return safeString(data[start:i]), i + 1, nil
			}
			// Has escapes, decode them
			return decodeEscapes(data[start:i]), i + 1, nil
		}
		if c == '\\' {
			hasEscape = true
			i++ // Skip next char
		}
	}

	return "", pos, errors.New("unterminated string")
}

func decodeEscapes(data []byte) string {
	buf := make([]byte, 0, len(data))
	i := 0
	for i < len(data) {
		if data[i] == '\\' && i+1 < len(data) {
			i++
			switch data[i] {
			case '"':
				buf = append(buf, '"')
			case '\\':
				buf = append(buf, '\\')
			case '/':
				buf = append(buf, '/')
			case 'b':
				buf = append(buf, '\b')
			case 'f':
				buf = append(buf, '\f')
			case 'n':
				buf = append(buf, '\n')
			case 'r':
				buf = append(buf, '\r')
			case 't':
				buf = append(buf, '\t')
			case 'u':
				// Unicode escape: \uXXXX
				if i+4 < len(data) {
					r := parseHex4(data[i+1 : i+5])
					if r >= 0 {
						// Encode rune as UTF-8
						var tmp [4]byte
						n := encodeRune(tmp[:], rune(r))
						buf = append(buf, tmp[:n]...)
						i += 4
					} else {
						buf = append(buf, data[i])
					}
				} else {
					buf = append(buf, data[i])
				}
			default:
				buf = append(buf, data[i])
			}
			i++
		} else {
			buf = append(buf, data[i])
			i++
		}
	}
	return string(buf)
}

func parseHex4(data []byte) int {
	var r int
	for _, c := range data {
		r <<= 4
		switch {
		case c >= '0' && c <= '9':
			r |= int(c - '0')
		case c >= 'a' && c <= 'f':
			r |= int(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			r |= int(c - 'A' + 10)
		default:
			return -1
		}
	}
	return r
}

func encodeRune(buf []byte, r rune) int {
	if r < 0x80 {
		buf[0] = byte(r)
		return 1
	}
	if r < 0x800 {
		buf[0] = byte(0xC0 | (r >> 6))
		buf[1] = byte(0x80 | (r & 0x3F))
		return 2
	}
	if r < 0x10000 {
		buf[0] = byte(0xE0 | (r >> 12))
		buf[1] = byte(0x80 | ((r >> 6) & 0x3F))
		buf[2] = byte(0x80 | (r & 0x3F))
		return 3
	}
	buf[0] = byte(0xF0 | (r >> 18))
	buf[1] = byte(0x80 | ((r >> 12) & 0x3F))
	buf[2] = byte(0x80 | ((r >> 6) & 0x3F))
	buf[3] = byte(0x80 | (r & 0x3F))
	return 4
}

func parseInt64(data []byte, pos int) (val int64, newPos int, err error) {
	start := pos
	var neg bool

	if pos < len(data) && data[pos] == '-' {
		neg = true
		pos++
	}

	if pos >= len(data) || data[pos] < '0' || data[pos] > '9' {
		return 0, start, errors.New("expected digit")
	}

	for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
		val = val*10 + int64(data[pos]-'0')
		pos++
	}

	if neg {
		val = -val
	}

	return val, pos, nil
}

func skipValue(data []byte, pos int) (int, error) {
	if pos >= len(data) {
		return pos, errors.New("unexpected end")
	}

	switch data[pos] {
	case '"':
		_, newPos, err := parseString(data, pos)
		return newPos, err
	case '{':
		return skipObject(data, pos)
	case '[':
		return skipArray(data, pos)
	case 't', 'f', 'n':
		return skipLiteral(data, pos)
	default:
		return skipNumber(data, pos)
	}
}

func skipObject(data []byte, pos int) (int, error) {
	if pos >= len(data) || data[pos] != '{' {
		return pos, errors.New("expected opening brace")
	}
	depth := 1
	pos++
	for pos < len(data) && depth > 0 {
		switch data[pos] {
		case '{':
			depth++
		case '}':
			depth--
		case '"':
			_, newPos, err := parseString(data, pos)
			if err != nil {
				return pos, err
			}
			pos = newPos
			continue
		}
		pos++
	}
	if depth != 0 {
		return pos, errors.New("unterminated object")
	}
	return pos, nil
}

func skipArray(data []byte, pos int) (int, error) {
	if pos >= len(data) || data[pos] != '[' {
		return pos, errors.New("expected opening bracket")
	}
	depth := 1
	pos++
	for pos < len(data) && depth > 0 {
		switch data[pos] {
		case '[':
			depth++
		case ']':
			depth--
		case '"':
			_, newPos, err := parseString(data, pos)
			if err != nil {
				return pos, err
			}
			pos = newPos
			continue
		}
		pos++
	}
	if depth != 0 {
		return pos, errors.New("unterminated array")
	}
	return pos, nil
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
	return pos, errors.New("unknown literal")
}

func skipNumber(data []byte, pos int) (int, error) {
	if pos < len(data) && data[pos] == '-' {
		pos++
	}
	for pos < len(data) && ((data[pos] >= '0' && data[pos] <= '9') || data[pos] == '.' || data[pos] == 'e' || data[pos] == 'E' || data[pos] == '+' || data[pos] == '-') {
		pos++
	}
	return pos, nil
}

// ParserPoolStats tracks pool usage statistics
type ParserPoolStats struct {
	Gets   uint64
	Puts   uint64
	Hits   uint64
	Misses uint64
}

var (
	// ticketParserPool is a thread-safe pool of parsers
	ticketParserPool sync.Pool

	// Pool statistics tracked with atomics for thread-safety
	parserPoolGets   uint64
	parserPoolPuts   uint64
	parserPoolHits   uint64
	parserPoolMisses uint64
)

// GetParserPoolStats returns current pool statistics
func GetParserPoolStats() ParserPoolStats {
	return ParserPoolStats{
		Gets:   atomic.LoadUint64(&parserPoolGets),
		Puts:   atomic.LoadUint64(&parserPoolPuts),
		Hits:   atomic.LoadUint64(&parserPoolHits),
		Misses: atomic.LoadUint64(&parserPoolMisses),
	}
}

// ParseTicketQuerySafe is a thread-safe wrapper that uses pooled parsers
func ParseTicketQuerySafe(data []byte) (TicketQuery, error) {
	atomic.AddUint64(&parserPoolGets, 1)
	if metrics.ParserPoolGets != nil {
		metrics.ParserPoolGets.Inc()
	}

	// Try to get a parser from the pool
	pooled := ticketParserPool.Get()
	var parser *ZeroAllocTicketParser
	if pooled != nil {
		atomic.AddUint64(&parserPoolHits, 1)
		if metrics.ParserPoolHits != nil {
			metrics.ParserPoolHits.Inc()
		}
		parser = pooled.(*ZeroAllocTicketParser)
	} else {
		atomic.AddUint64(&parserPoolMisses, 1)
		if metrics.ParserPoolMisses != nil {
			metrics.ParserPoolMisses.Inc()
		}
		parser = NewZeroAllocTicketParser()
	}

	// Parse the data
	result, err := parser.Parse(data)

	// Return parser to pool
	ticketParserPool.Put(parser)
	atomic.AddUint64(&parserPoolPuts, 1)
	if metrics.ParserPoolPuts != nil {
		metrics.ParserPoolPuts.Inc()
	}

	return result, err
}
