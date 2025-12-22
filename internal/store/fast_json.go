package store

import (
	gojson "github.com/goccy/go-json"
)

// FastParseTicketQuery parses ticket JSON using goccy/go-json
// which is 2-3x faster than encoding/json for this use case.
// It's a drop-in replacement with identical semantics.
func FastParseTicketQuery(ticket []byte) (TicketQuery, error) {
	var query TicketQuery
	if len(ticket) == 0 {
		return query, nil
	}
	err := gojson.Unmarshal(ticket, &query)
	return query, err
}
