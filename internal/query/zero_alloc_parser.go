package query

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/core"
	"github.com/rs/zerolog"
)

// Hash returns a unique string representation of a filter for caching purposes.
func FilterHash(f core.Filter) string {
	h := f.Field + ":" + f.Operator + ":" + f.Value + ":" + f.Logic
	if len(f.Filters) > 0 {
		h += "("
		for i := range f.Filters {
			h += FilterHash(f.Filters[i])
			if i < len(f.Filters)-1 {
				h += ","
			}
		}
		h += ")"
	}
	return h
}

// ZeroAllocTicketParser parses TicketQuery JSON with zero allocations
// for the common case (no escape sequences).
type ZeroAllocTicketParser struct {
	result          TicketQuery
	filters         []Filter
	windowFunctions []WindowFunction
	ctes            []core.CTE
	searchParser    *ZeroAllocVectorSearchParser
	logger          zerolog.Logger
}

// NewZeroAllocTicketParser creates a new reusable parser
func NewZeroAllocTicketParser(logger *zerolog.Logger) *ZeroAllocTicketParser {
	return &ZeroAllocTicketParser{
		filters:         make([]Filter, 0, 16),
		windowFunctions: make([]WindowFunction, 0, 4),
		ctes:            make([]core.CTE, 0, 4),
		searchParser:    NewZeroAllocVectorSearchParser(768, logger), // Default max dims
		logger:          *logger,
	}
}

// Parse parses the JSON data into a TicketQuery
func (p *ZeroAllocTicketParser) Parse(data []byte) (TicketQuery, error) {
	// Reset result state
	p.result = TicketQuery{}
	p.filters = p.filters[:0]
	p.windowFunctions = p.windowFunctions[:0]

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
			if len(p.windowFunctions) > 0 {
				p.result.WindowFunctions = make([]WindowFunction, len(p.windowFunctions))
				copy(p.result.WindowFunctions, p.windowFunctions)
			} else {
				p.result.WindowFunctions = nil
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
		case "window_functions":
			newPos, err := p.parseWindowFunctions(data, i)
			if err != nil {
				return p.result, err
			}
			i = newPos
		case "search":
			if i+4 <= len(data) && string(data[i:i+4]) == "null" {
				i += 4
			} else {
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
			}
		case "with", "ctes":
			newPos, err := p.parseCTEs(data, i)
			if err != nil {
				return p.result, err
			}
			i = newPos
		case "vector":
			if i+4 <= len(data) && string(data[i:i+4]) == "null" {
				i += 4
			} else {
				// Use the search parser's vector parsing method (or copy it)
				// For now, extract and use searchParser to avoid duplication
				newPos, err := p.searchParser.ParseVectorField(data, i, &p.result.Vector)
				if err != nil {
					return p.result, err
				}
				i = newPos
			}
		case "k":
			val, newPos, err := parseInt64(data, i)
			if err != nil {
				return p.result, err
			}
			p.result.K = int(val)
			i = newPos
		case "geo_search":
			if i+4 <= len(data) && string(data[i:i+4]) == "null" {
				i += 4
			} else {
				start := i
				newPos, err := skipObject(data, i)
				if err != nil {
					return p.result, err
				}
				var req core.GeoSearchRequest
				if err := json.Unmarshal(data[start:newPos], &req); err != nil {
					return p.result, err
				}
				p.result.GeoSearch = &req
				i = newPos
			}
		case "search_by_id":
			if i+4 <= len(data) && string(data[i:i+4]) == "null" {
				i += 4
			} else {
				// Parse VectorSearchByIDRequest from JSON object
				start := i
				newPos, err := skipObject(data, i)
				if err != nil {
					return p.result, err
				}
				// Parse the JSON object into VectorSearchByIDRequest
				var req core.VectorSearchByIDRequest
				if err := parseSearchByIDRequest(data[start:newPos], &req); err != nil {
					return p.result, err
				}
				p.result.SearchByID = &req
				i = newPos
			}
		case "recommend":
			if i+4 <= len(data) && string(data[i:i+4]) == "null" {
				i += 4
			} else {
				// Parse RecommendRequest from JSON object
				start := i
				newPos, err := skipObject(data, i)
				if err != nil {
					return p.result, err
				}
				var req core.RecommendRequest
				if err := parseRecommendRequest(data[start:newPos], &req); err != nil {
					return p.result, err
				}
				p.result.Recommend = &req
				i = newPos
			}
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

func (p *ZeroAllocTicketParser) parseWindowFunctions(data []byte, pos int) (int, error) {
	wfs, newPos, err := parseWindowFunctionsShared(data, pos)
	if err != nil {
		return pos, err
	}
	p.windowFunctions = wfs
	return newPos, nil
}

func (p *ZeroAllocTicketParser) parseFilters(data []byte, pos int) (int, error) {
	return parseFilterArrayRecursive(data, pos, &p.filters, p)
}

func (p *ZeroAllocTicketParser) parseCTEs(data []byte, pos int) (int, error) {
	pos = skipWhitespace(data, pos)
	if data[pos] != '[' {
		return pos, errors.New("expected [ for ctes")
	}
	pos++

	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if data[pos] == ']' {
			return pos + 1, nil
		}

		var cte core.CTE
		newPos, err := p.parseCTEObject(data, pos, &cte)
		if err != nil {
			return pos, err
		}
		p.result.CTEs = append(p.result.CTEs, cte)
		pos = newPos

		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, nil
}

func (p *ZeroAllocTicketParser) parseCTEObject(data []byte, pos int, cte *core.CTE) (int, error) {
	if data[pos] != '{' {
		return pos, errors.New("expected { for cte object")
	}
	pos++

	for pos < len(data) {
		pos = skipWhitespace(data, pos)
		if data[pos] == '}' {
			return pos + 1, nil
		}

		key, newPos, err := parseString(data, pos)
		if err != nil {
			return pos, err
		}
		pos = newPos

		pos = skipWhitespace(data, pos)
		if data[pos] != ':' {
			return pos, errors.New("expected : in cte object")
		}
		pos++
		pos = skipWhitespace(data, pos)

		switch key {
		case "name":
			val, newPos, err := parseString(data, pos)
			if err != nil {
				return pos, err
			}
			cte.Name = val
			pos = newPos
		case "search":
			start := pos
			newPos, err := skipObject(data, pos)
			if err != nil {
				return pos, err
			}
			searchReq, err := p.searchParser.Parse(data[start:newPos])
			if err != nil {
				return pos, err
			}
			cte.Search = &searchReq
			pos = newPos
		default:
			pos, err = skipValue(data, pos)
			if err != nil {
				return pos, err
			}
		}

		pos = skipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, nil
}

var (
	parserPool = sync.Pool{
		New: func() interface{} {
			nopLogger := zerolog.Nop()
			return NewZeroAllocTicketParser(&nopLogger)
		},
	}
	poolGets uint64
	poolPuts uint64
)

// ParseTicketQuerySafe uses a pool of parsers to thread-safely parse TicketQuery
func ParseTicketQuerySafe(data []byte) (TicketQuery, error) {
	parser := parserPool.Get().(*ZeroAllocTicketParser)
	atomic.AddUint64(&poolGets, 1)

	res, err := parser.Parse(data)

	parserPool.Put(parser)
	atomic.AddUint64(&poolPuts, 1)
	return res, err
}

type ParserPoolStats struct {
	Gets uint64
	Puts uint64
}

func GetParserPoolStats() ParserPoolStats {
	return ParserPoolStats{
		Gets: atomic.LoadUint64(&poolGets),
		Puts: atomic.LoadUint64(&poolPuts),
	}
}
