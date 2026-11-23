package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// ---------- Pure-Go GraphQL Lexer, Recursive-Descent Parser & Executor ----------

type gqlTokKind int

const (
	tokEOF gqlTokKind = iota
	tokName
	tokInt
	tokFloat
	tokString
	tokLBrace
	tokRBrace
	tokLParen
	tokRParen
	tokLBrack
	tokRBrack
	tokColon
	tokEqual
	tokBang
	tokDollar
)

type gqlTok struct {
	kind gqlTokKind
	val  string
}

func lexGraphQL(src string) ([]gqlTok, error) {
	var toks []gqlTok
	i := 0
	for i < len(src) {
		ch := src[i]
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == ',' {
			i++
			continue
		}
		if ch == '#' {
			for i < len(src) && src[i] != '\n' {
				i++
			}
			continue
		}
		switch ch {
		case '{':
			toks = append(toks, gqlTok{tokLBrace, "{"})
			i++
		case '}':
			toks = append(toks, gqlTok{tokRBrace, "}"})
			i++
		case '(':
			toks = append(toks, gqlTok{tokLParen, "("})
			i++
		case ')':
			toks = append(toks, gqlTok{tokRParen, ")"})
			i++
		case '[':
			toks = append(toks, gqlTok{tokLBrack, "["})
			i++
		case ']':
			toks = append(toks, gqlTok{tokRBrack, "]"})
			i++
		case ':':
			toks = append(toks, gqlTok{tokColon, ":"})
			i++
		case '=':
			toks = append(toks, gqlTok{tokEqual, "="})
			i++
		case '!':
			toks = append(toks, gqlTok{tokBang, "!"})
			i++
		case '$':
			toks = append(toks, gqlTok{tokDollar, "$"})
			i++
		case '"':
			start := i + 1
			i++
			var sb strings.Builder
			closed := false
			for i < len(src) {
				if src[i] == '\\' && i+1 < len(src) {
					sb.WriteByte(src[i+1])
					i += 2
					continue
				}
				if src[i] == '"' {
					i++
					closed = true
					break
				}
				sb.WriteByte(src[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated string literal at %d", start)
			}
			toks = append(toks, gqlTok{tokString, sb.String()})
		default:
			if unicode.IsLetter(rune(ch)) || ch == '_' {
				start := i
				for i < len(src) && (unicode.IsLetter(rune(src[i])) || unicode.IsDigit(rune(src[i])) || src[i] == '_') {
					i++
				}
				toks = append(toks, gqlTok{tokName, src[start:i]})
			} else if ch == '-' || unicode.IsDigit(rune(ch)) {
				start := i
				if ch == '-' {
					i++
				}
				isFloat := false
				for i < len(src) && (unicode.IsDigit(rune(src[i])) || src[i] == '.' || src[i] == 'e' || src[i] == 'E' || src[i] == '+' || src[i] == '-') {
					if src[i] == '.' || src[i] == 'e' || src[i] == 'E' {
						isFloat = true
					}
					if (src[i] == '+' || src[i] == '-') && i > start && src[i-1] != 'e' && src[i-1] != 'E' {
						break
					}
					i++
				}
				if isFloat {
					toks = append(toks, gqlTok{tokFloat, src[start:i]})
				} else {
					toks = append(toks, gqlTok{tokInt, src[start:i]})
				}
			} else {
				return nil, fmt.Errorf("unexpected character %q at offset %d", ch, i)
			}
		}
	}
	toks = append(toks, gqlTok{kind: tokEOF})
	return toks, nil
}

type gqlOp struct {
	opType   string // "query" or "mutation"
	name     string
	defaults map[string]any
	sels     []gqlSelection
}

type gqlSelection struct {
	alias string
	name  string
	args  map[string]any
	sels  []gqlSelection
}

type gqlVarRef string

type gqlParser struct {
	toks []gqlTok
	pos  int
}

func (p *gqlParser) peek() gqlTok {
	if p.pos >= len(p.toks) {
		return gqlTok{kind: tokEOF}
	}
	return p.toks[p.pos]
}

func (p *gqlParser) next() gqlTok {
	t := p.peek()
	if p.pos < len(p.toks) {
		p.pos++
	}
	return t
}

func (p *gqlParser) expect(k gqlTokKind) (gqlTok, error) {
	t := p.next()
	if t.kind != k {
		return t, fmt.Errorf("unexpected token %q (kind %d), expected kind %d", t.val, t.kind, k)
	}
	return t, nil
}

func parseGraphQL(src string) ([]gqlOp, error) {
	toks, err := lexGraphQL(src)
	if err != nil {
		return nil, err
	}
	p := &gqlParser{toks: toks}
	var ops []gqlOp
	for p.peek().kind != tokEOF {
		op := gqlOp{opType: "query", defaults: make(map[string]any)}
		if p.peek().kind == tokLBrace {
			sels, err := p.parseSelectionSet()
			if err != nil {
				return nil, err
			}
			op.sels = sels
			ops = append(ops, op)
			continue
		}
		if p.peek().kind != tokName {
			return nil, fmt.Errorf("expected query or mutation, got %q", p.peek().val)
		}
		kw := p.next().val
		if kw != "query" && kw != "mutation" && kw != "subscription" {
			return nil, fmt.Errorf("unsupported operation %q", kw)
		}
		op.opType = kw
		if p.peek().kind == tokName {
			op.name = p.next().val
		}
		if p.peek().kind == tokLParen {
			if err := p.parseVarDefs(op.defaults); err != nil {
				return nil, err
			}
		}
		sels, err := p.parseSelectionSet()
		if err != nil {
			return nil, err
		}
		op.sels = sels
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return nil, errors.New("empty GraphQL document")
	}
	return ops, nil
}

func (p *gqlParser) parseVarDefs(defaults map[string]any) error {
	p.next() // '('
	for p.peek().kind != tokRParen {
		if _, err := p.expect(tokDollar); err != nil {
			return err
		}
		nameTok, err := p.expect(tokName)
		if err != nil {
			return err
		}
		if _, err := p.expect(tokColon); err != nil {
			return err
		}
		if err := p.skipTypeRef(); err != nil {
			return err
		}
		if p.peek().kind == tokEqual {
			p.next()
			v, err := p.parseValue()
			if err != nil {
				return err
			}
			defaults[nameTok.val] = v
		}
	}
	p.next() // ')'
	return nil
}

func (p *gqlParser) skipTypeRef() error {
	if p.peek().kind == tokLBrack {
		p.next()
		if err := p.skipTypeRef(); err != nil {
			return err
		}
		if _, err := p.expect(tokRBrack); err != nil {
			return err
		}
	} else {
		if _, err := p.expect(tokName); err != nil {
			return err
		}
	}
	if p.peek().kind == tokBang {
		p.next()
	}
	return nil
}

func (p *gqlParser) parseSelectionSet() ([]gqlSelection, error) {
	if _, err := p.expect(tokLBrace); err != nil {
		return nil, err
	}
	var sels []gqlSelection
	for p.peek().kind != tokRBrace {
		if p.peek().kind == tokEOF {
			return nil, errors.New("unterminated selection set")
		}
		nTok, err := p.expect(tokName)
		if err != nil {
			return nil, err
		}
		sel := gqlSelection{name: nTok.val, args: make(map[string]any)}
		if p.peek().kind == tokColon {
			p.next()
			fTok, err := p.expect(tokName)
			if err != nil {
				return nil, err
			}
			sel.alias = sel.name
			sel.name = fTok.val
		}
		if p.peek().kind == tokLParen {
			args, err := p.parseArgs()
			if err != nil {
				return nil, err
			}
			sel.args = args
		}
		if p.peek().kind == tokLBrace {
			sub, err := p.parseSelectionSet()
			if err != nil {
				return nil, err
			}
			sel.sels = sub
		}
		sels = append(sels, sel)
	}
	p.next() // '}'
	return sels, nil
}

func (p *gqlParser) parseArgs() (map[string]any, error) {
	p.next() // '('
	out := make(map[string]any)
	for p.peek().kind != tokRParen {
		if p.peek().kind == tokEOF {
			return nil, errors.New("unterminated argument list")
		}
		nTok, err := p.expect(tokName)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokColon); err != nil {
			return nil, err
		}
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		out[nTok.val] = v
	}
	p.next() // ')'
	return out, nil
}

func (p *gqlParser) parseValue() (any, error) {
	t := p.peek()
	switch t.kind {
	case tokDollar:
		p.next()
		nt, err := p.expect(tokName)
		if err != nil {
			return nil, err
		}
		return gqlVarRef(nt.val), nil
	case tokString:
		p.next()
		return t.val, nil
	case tokInt:
		p.next()
		v, err := strconv.ParseInt(t.val, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case tokFloat:
		p.next()
		v, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case tokName:
		p.next()
		switch t.val {
		case "true":
			return true, nil
		case "false":
			return false, nil
		case "null":
			return nil, nil
		default:
			return t.val, nil
		}
	case tokLBrack:
		p.next()
		var list []any
		for p.peek().kind != tokRBrack {
			if p.peek().kind == tokEOF {
				return nil, errors.New("unterminated list literal")
			}
			item, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			list = append(list, item)
		}
		p.next()
		return list, nil
	case tokLBrace:
		p.next()
		obj := make(map[string]any)
		for p.peek().kind != tokRBrace {
			if p.peek().kind == tokEOF {
				return nil, errors.New("unterminated input object literal")
			}
			kt, err := p.expect(tokName)
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokColon); err != nil {
				return nil, err
			}
			val, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			obj[kt.val] = val
		}
		p.next()
		return obj, nil
	default:
		return nil, fmt.Errorf("unexpected value token %q", t.val)
	}
}

func resolveGQLValue(v any, vars map[string]any) any {
	switch x := v.(type) {
	case gqlVarRef:
		return vars[string(x)]
	case []any:
		out := make([]any, len(x))
		for i, it := range x {
			out[i] = resolveGQLValue(it, vars)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = resolveGQLValue(val, vars)
		}
		return out
	default:
		return v
	}
}

func resolveGQLArgs(args, vars map[string]any) map[string]any {
	resolved := make(map[string]any, len(args))
	for k, v := range args {
		resolved[k] = resolveGQLValue(v, vars)
	}
	return resolved
}

func argString(args map[string]any, key string) string {
	v, ok := args[key]
	if !ok || v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func argInt(args map[string]any, key string, def int) int {
	v, ok := args[key]
	if !ok || v == nil {
		return def
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i)
		}
	}
	return def
}

func argFloat32(args map[string]any, key string, def float32) float32 {
	v, ok := args[key]
	if !ok || v == nil {
		return def
	}
	switch n := v.(type) {
	case int:
		return float32(n)
	case int64:
		return float32(n)
	case float64:
		return float32(n)
	}
	return def
}

func argFloat64Slice(v any) []float32 {
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]float32, 0, len(items))
	for _, it := range items {
		switch n := it.(type) {
		case float64:
			out = append(out, float32(n))
		case int64:
			out = append(out, float32(n))
		case int:
			out = append(out, float32(n))
		}
	}
	return out
}

func argStringSlice(v any) []string {
	switch items := v.(type) {
	case []string:
		return items
	case []any:
		out := make([]string, 0, len(items))
		for _, it := range items {
			out = append(out, fmt.Sprint(it))
		}
		return out
	default:
		return nil
	}
}

func projectSelectionSet(val any, sels []gqlSelection) any {
	if len(sels) == 0 || val == nil {
		return val
	}
	raw, err := json.Marshal(val)
	if err != nil {
		return val
	}
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return val
	}
	return projectJSONValue(generic, sels)
}

func projectJSONValue(v any, sels []gqlSelection) any {
	if len(sels) == 0 || v == nil {
		return v
	}
	switch node := v.(type) {
	case []any:
		out := make([]any, len(node))
		for i, item := range node {
			out[i] = projectJSONValue(item, sels)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(sels))
		for _, sel := range sels {
			key := sel.name
			if sel.alias != "" {
				key = sel.alias
			}
			if val, ok := findCamelOrSnakeField(node, sel.name); ok {
				out[key] = projectJSONValue(val, sel.sels)
			} else {
				out[key] = nil
			}
		}
		return out
	default:
		return v
	}
}

func findCamelOrSnakeField(m map[string]any, name string) (any, bool) {
	if v, ok := m[name]; ok {
		return v, true
	}
	snake := toSnakeCase(name)
	if v, ok := m[snake]; ok {
		return v, true
	}
	return nil, false
}

func toSnakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) && i > 0 {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

func graphqlSchemaIntrospection() map[string]any {
	types := []map[string]any{
		{
			"name": "Query",
			"kind": "OBJECT",
			"fields": []map[string]any{
				{"name": "health", "type": "HealthStatus"},
				{"name": "namespaces", "type": "[String!]!"},
				{"name": "datasets", "type": "[DatasetInfo!]!"},
				{"name": "dataset", "type": "DatasetInfo"},
				{"name": "search", "type": "[SearchResult!]!"},
				{"name": "searchById", "type": "[SearchResult!]!"},
				{"name": "hybridSearch", "type": "[SearchResult!]!"},
				{"name": "textSearch", "type": "AdvancedTextSearchResponse"},
				{"name": "geoSearch", "type": "[SearchResult!]!"},
				{"name": "temporalSearch", "type": "[SearchResult!]!"},
				{"name": "multimodalSearch", "type": "[SearchResult!]!"},
				{"name": "rangeSearch", "type": "[SearchResult!]!"},
				{"name": "federatedSearch", "type": "[SearchResult!]!"},
				{"name": "federatedCollections", "type": "[CollectionInfo!]!"},
				{"name": "recommend", "type": "[SearchResult!]!"},
				{"name": "aggregate", "type": "[AggregationRow!]!"},
				{"name": "usage", "type": "[TenantUsageReport!]!"},
				{"name": "cloudClusters", "type": "[CloudCluster!]!"},
				{"name": "cloudSla", "type": "SLAReport"},
				{"name": "explainSearch", "type": "SearchExplanationReport"},
				{"name": "provenance", "type": "[VectorProvenance!]!"},
				{"name": "searchAnalytics", "type": "SearchAnalyticsSnapshot"},
				{"name": "materializedViews", "type": "[MaterializedViewSnapshot!]!"},
				{"name": "schemaRegistry", "type": "DatasetSchemaSpec"},
				{"name": "clusterTopology", "type": "ElasticTopologySnapshot"},
				{"name": "streamConnectors", "type": "[StreamConnectorStats!]!"},
				{"name": "backups", "type": "[BackupMetadata!]!"},
				{"name": "rewriteQuery", "type": "QueryRewriteResult"},
			},
		},
		{
			"name": "Mutation",
			"kind": "OBJECT",
			"fields": []map[string]any{
				{"name": "createNamespace", "type": "StatusResult"},
				{"name": "deleteNamespace", "type": "StatusResult"},
				{"name": "upsertVectors", "type": "UpsertResult"},
				{"name": "deleteDataset", "type": "StatusResult"},
				{"name": "deleteVector", "type": "StatusResult"},
				{"name": "createCloudCluster", "type": "CloudCluster"},
				{"name": "promoteCloudRegion", "type": "CloudCluster"},
				{"name": "recordProvenance", "type": "StatusResult"},
				{"name": "createMaterializedView", "type": "MaterializedViewSnapshot"},
				{"name": "refreshMaterializedView", "type": "MaterializedViewSnapshot"},
				{"name": "registerSchema", "type": "SchemaVersionResult"},
				{"name": "triggerBackup", "type": "BackupMetadata"},
				{"name": "restoreBackup", "type": "PITRRestoreReport"},
				{"name": "createStreamConnector", "type": "StreamConnectorStats"},
				{"name": "stopStreamConnector", "type": "StatusResult"},
				{"name": "setTenantQuota", "type": "StatusResult"},
				{"name": "triggerShardSplit", "type": "ShardSplitResult"},
				{"name": "triggerShardMerge", "type": "ShardMergeResult"},
				{"name": "triggerRebalance", "type": "ShardRebalanceResult"},
				{"name": "ingestMedia", "type": "UpsertResult"},
				{"name": "rerank", "type": "[RerankCandidate!]!"},
			},
		},
		{"name": "HealthStatus", "kind": "OBJECT"},
		{"name": "DatasetInfo", "kind": "OBJECT"},
		{"name": "SearchResult", "kind": "OBJECT"},
		{"name": "CloudCluster", "kind": "OBJECT"},
		{"name": "SLAReport", "kind": "OBJECT"},
	}
	return map[string]any{
		"queryType":    map[string]any{"name": "Query"},
		"mutationType": map[string]any{"name": "Mutation"},
		"types":        types,
	}
}
