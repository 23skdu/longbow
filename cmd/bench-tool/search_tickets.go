package main

import (
	"fmt"
	"math/rand"
	"strings"
)

// defaultTemporalAsOf is 2100-01-01T00:00:00Z in Unix nanoseconds. as-of search
// is inclusive of everything at or before the timestamp, so a fixed far-future
// value is both deterministic and non-empty regardless of when the corpus was
// generated. Fixed-value determinism matters more here than semantic realism: the
// mode exists to be compared against a baseline, not to model a point in time.
// defaultSeed is the fixed seed for corpus, queries and ByID ids (R16, H7).
// Any value works as long as it does not change between runs; 42 is arbitrary and
// fixed. The previous behaviour seeded from time.Now().UnixNano() per chunk, so
// no two runs saw the same corpus and neither index shape nor ingest throughput
// was comparable across runs - which is where the 40%+ per-mode variance came
// from.
const defaultSeed int64 = 42

// RunSeed is the seed for this run, set once from -seed. Package-level so the
// few helpers that build tickets or single records do not each need it threaded
// through; it is written once during flag parsing and read-only afterwards.
var RunSeed = defaultSeed

const defaultTemporalAsOf int64 = 4102444800000000000

// TemporalAsOfNanos is the as-of timestamp used by every Temporal ticket in this
// run. Set once from -temporal-asof-nanos; deliberately not read from the clock
// per query.
var TemporalAsOfNanos = defaultTemporalAsOf

// ReusableSearchState represents a reusable search state value.
type ReusableSearchState struct {
	vector []float32
	buf    []byte
}

// NewReusableSearchState constructs a new ReusableSearchState.
func NewReusableSearchState(maxDim int) *ReusableSearchState {
	return &ReusableSearchState{
		vector: make([]float32, maxDim*2), // 2*dim for complex types
		buf:    make([]byte, 0, 1024*64),  // 64KB should fit most vectors
	}
}

// corpusValueRange is the half-open interval the generated corpus occupies for a
// given dtype. It is the single source of truth for that interval: the corpus
// generator draws from it and BuildSearchTicket draws its queries from it too.
//
// The two must agree because a query has to live in the same numeric domain as
// the vectors it is compared against. The server narrows a float32 query into
// the dataset's integer type with a rounding cast that clamps at the type's
// limits, so a query drawn from [0,1) against an int8 corpus spanning [0,127)
// arrives as 128 zeros and the search degenerates into "nearest node to the
// origin" - the same answer for every query, measured as if it were a search.
// That is what made the 100k matrix report int8 at 1162 QPS and uint8 at 3974
// QPS while both are one byte wide; see docs/development/roadmap.md item 1 (R32).
func corpusValueRange(dtype string) (lo, hi float32) {
	switch dtype {
	case "int8":
		return 0, 127
	case "uint8":
		return 0, 255
	case "int16", "uint16", "int32", "uint32", "int64", "uint64":
		return 0, 1000
	default:
		// float16, float32, float64, complex64, complex128, turboquant.
		return 0, 1
	}
}

// corpusValueBound is the exclusive upper bound of corpusValueRange, as an int,
// for the integer element types whose corpus is drawn with rng.Intn.
func corpusValueBound(dtype string) int {
	_, hi := corpusValueRange(dtype)
	return int(hi)
}

func (s *ReusableSearchState) fillQueryVector(dim int, dtype string, mode string, queryIdx int) (queryLen int, qLo, qHi float32) {
	queryLen = dim
	if dtype == "complex64" || dtype == "complex128" {
		queryLen = dim * 2
	}

	// Randomize vector in-place
	// One stream per (mode, query index): reproducible, and independent enough
	// that neighbouring queries are not near-duplicates.
	qLo, qHi = corpusValueRange(dtype)
	qRng := rand.New(rand.NewSource(RunSeed + int64(queryIdx)*2654435761 + int64(len(mode))*97)) // #nosec G404 -- benchmark data
	span := float64(qHi - qLo)
	for i := 0; i < queryLen; i++ {
		s.vector[i] = qLo + float32(qRng.Float64()*span) // #nosec G404 -- benchmark data
	}
	return queryLen, qLo, qHi
}

func (s *ReusableSearchState) appendVectorField(field string, queryLen int) {
	s.buf = append(s.buf, `,"`...)
	s.buf = append(s.buf, field...)
	s.buf = append(s.buf, `":[`...)
	for i := 0; i < queryLen; i++ {
		if i > 0 {
			s.buf = append(s.buf, ',')
		}
		s.buf = fmt.Appendf(s.buf, "%g", s.vector[i])
	}
	s.buf = append(s.buf, ']')
}

// BuildSearchTicket builds a search ticket for one query.
//
// queryIdx makes the query vector a deterministic function of (mode, query
// index) rather than of the global math/rand, which Go seeds randomly per
// process (R16, H7). Two runs of the same binary therefore issue byte-identical
// queries, which is the precondition for comparing two runs at all.
func (s *ReusableSearchState) BuildSearchTicket(dataset string, dim int, dtype string, mode string, k int, queryIdx int) []byte {
	s.buf = s.buf[:0]
	s.buf = append(s.buf, `{"search":{"dataset":"`...)
	s.buf = append(s.buf, dataset...)
	s.buf = append(s.buf, `","k":`...)
	s.buf = append(s.buf, fmt.Sprintf("%d", k)...)

	queryLen, _, _ := s.fillQueryVector(dim, dtype, mode, queryIdx)

	switch mode {
	case "Dense":
		s.appendVectorField("vector", queryLen)
	case "Hybrid":
		s.appendVectorField("vector", queryLen)
		s.buf = append(s.buf, `,"text_query":"electronics","alpha":0.5`...)
	case "Filtered", "FilteredBool", "FilteredString":
		s.appendVectorField("vector", queryLen)
		s.buf = append(s.buf, `,"filters":[`...)
		switch mode {
		case "Filtered":
			s.buf = append(s.buf, `{"field":"id","operator":">","value":"10"}`...)
		case "FilteredBool":
			s.buf = append(s.buf, `{"field":"active","operator":"==","value":"true"}`...)
		default:
			s.buf = append(s.buf, `{"field":"category","operator":"==","value":"electronics"}`...)
		}
		s.buf = append(s.buf, ']')
	case "GraphRAG", "GlobalGraphRAG":
		s.appendVectorField("vector", queryLen)
		s.buf = append(s.buf, `,"graph_alpha":0.5`...)
	case "Sparse":
		s.buf = append(s.buf, `,"text_query":"electronics","alpha":0.0`...)
	case "LearnedIndex":
		s.appendVectorField("vector", queryLen)
		s.buf = append(s.buf, `,"enable_learned_index":true`...)
	}

	s.buf = append(s.buf, `}}`...)
	return s.buf
}

// BuildSpecialTicket runs the build special ticket operation.
func (s *ReusableSearchState) BuildSpecialTicket(dataset string, mode string, queryIdx int, corpusSize int) []byte {
	s.buf = s.buf[:0]
	switch mode {
	case "Recommend":
		s.buf = append(s.buf, `{"recommend":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		// Use first 3 IDs from dataset as seeds (IDs are generated as "0", "1", "2", etc.)
		s.buf = append(s.buf, `","k":10,"seed_ids":["0","1","2"],"max_hops":3,"decay":0.8,"alpha":0.5}}`...)
	case "Geo":
		s.buf = append(s.buf, `{"geo_search":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = append(s.buf, `","k":10,"center":{"lat":40.7128,"lon":-74.0060},"radius_km":50.0,"search_type":"radius"}}`...)
	case "Temporal":
		s.buf = append(s.buf, `{"temporal_search":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = append(s.buf, `","k":10,"search_type":"as_of","timestamp":`...)
		// Fixed, not time.Now(): the server keys its temporal result cache on
		// (timestamp, k), so a per-query clock reading guarantees a miss and an
		// LRU insert for every single query. See -temporal-asof-nanos.
		s.buf = append(s.buf, fmt.Sprintf("%d", TemporalAsOfNanos)...)
		s.buf = append(s.buf, `}}`...)
	case "ByID":
		// R16, H6: this was hardcoded to id "0", so every query hit one
		// permanently hot node and measured the mode's cache hit rather than
		// its search - a 7x swing between runs. Derived from the query index
		// and wrapped into the corpus, so consecutive queries touch different
		// ids and the mode is reproducible.
		byID := queryIdx
		if corpusSize > 0 {
			byID = queryIdx % corpusSize
		}
		s.buf = append(s.buf, `{"search_by_id":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = append(s.buf, `","k":10,"id":"`...)
		s.buf = append(s.buf, fmt.Sprintf("%d", byID)...)
		s.buf = append(s.buf, `"}}`...)
	case "GeoBox":
		s.buf = append(s.buf, `{"geo_search":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = append(s.buf, `","k":10,"box":{"min_lat":40.70,"max_lat":40.82,"min_lon":-74.02,"max_lon":-73.90},"search_type":"box"}}`...)
	case "TemporalRange":
		span := int64(corpusSize)
		if span < 1000 {
			span = 1000
		}
		s.buf = append(s.buf, `{"temporal_search":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = append(s.buf, `","k":10,"search_type":"range","start_time":`...)
		s.buf = fmt.Appendf(s.buf, "%d", TemporalAsOfNanos-span-10)
		s.buf = append(s.buf, `,"end_time":`...)
		s.buf = fmt.Appendf(s.buf, "%d", TemporalAsOfNanos)
		s.buf = append(s.buf, `}}`...)
	case "TemporalWindow":
		s.buf = append(s.buf, `{"temporal_search":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = append(s.buf, `","k":10,"search_type":"sliding_window","window_size":100}}`...)
	}
	return s.buf
}

var allSearchModes = []string{
	"Dense", "Hybrid", "Filtered", "FilteredBool", "FilteredString",
	"Sparse", "ByID", "Range", "AdvancedText", "GraphRAG", "GlobalGraphRAG",
	"Recommend", "Geo", "GeoBox", "GeoHybrid", "Temporal", "TemporalRange",
	"TemporalWindow", "MultiModal", "Federated", "LearnedIndex",
}

var searchModeAliases = map[string]string{
	"text":               "AdvancedText",
	"fulltext":           "AdvancedText",
	"advancedtextsearch": "AdvancedText",
	"rangesearch":        "Range",
	"federatedsearch":    "Federated",
	"georadius":          "Geo",
	"temporalasof":       "Temporal",
	"multimodalsearch":   "MultiModal",
}

func resolveSearchModes(raw string) []string {
	if raw == "all" {
		return append([]string(nil), allSearchModes...)
	}
	var modes []string
	seen := make(map[string]bool)
	normReplacer := strings.NewReplacer("_", "", "-", "")
	for _, m := range strings.Split(raw, ",") {
		norm := normReplacer.Replace(strings.ToLower(strings.TrimSpace(m)))
		if norm == "" {
			continue
		}
		if alias, ok := searchModeAliases[norm]; ok {
			if !seen[alias] {
				modes = append(modes, alias)
				seen[alias] = true
			}
			continue
		}
		for _, am := range allSearchModes {
			if norm == strings.ToLower(am) && !seen[am] {
				modes = append(modes, am)
				seen[am] = true
				break
			}
		}
	}
	return modes
}

// BuildVectorSpecialTicket builds non-standard DoGet tickets that also carry a deterministic query vector.
func (s *ReusableSearchState) BuildVectorSpecialTicket(dataset string, dim int, dtype string, mode string, k int, queryIdx int) []byte {
	s.buf = s.buf[:0]
	queryLen, qLo, qHi := s.fillQueryVector(dim, dtype, mode, queryIdx)
	span := float64(qHi - qLo)
	switch mode {
	case "Range":
		threshold := float32(0.25 * span * span * float64(queryLen))
		if threshold < 1.0 {
			threshold = 1.0
		}
		s.buf = append(s.buf, `{"range_search":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = fmt.Appendf(s.buf, `","threshold":%g,"max_results":%d`, threshold, k)
		s.appendVectorField("vector", queryLen)
		s.buf = append(s.buf, `}}`...)
	case "GeoHybrid":
		s.buf = append(s.buf, `{"geo_search":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = fmt.Appendf(s.buf, `","k":%d,"center":{"lat":40.7128,"lon":-74.0060},"radius_km":50.0,"search_type":"hybrid"`, k)
		s.appendVectorField("query_vector", queryLen)
		s.buf = append(s.buf, `}}`...)
	case "Federated":
		s.buf = append(s.buf, `{"federated_search":{"collections":["`...)
		s.buf = append(s.buf, dataset...)
		s.buf = fmt.Appendf(s.buf, `"],"k":%d,"rrf_k":60`, k)
		s.appendVectorField("vector", queryLen)
		s.buf = append(s.buf, `}}`...)
	}
	return s.buf
}
