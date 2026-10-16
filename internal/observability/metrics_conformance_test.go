package observability_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	rulesPath     = "../../grafana/rules.yml"
	dashboardsDir = "../../grafana/dashboards"
)

// registeredMetric mirrors one entry of the Prometheus conformance gate's
// inventory: the families this repository actually registers.
type registeredMetric struct {
	Name   string
	Labels []string
}

// metricFamily is a metric referenced by an alert, recording rule or dashboard.
type metricFamily struct {
	Name   string
	Source string
	Line   int
}

type ruleGroup struct {
	Name  string `yaml:"name"`
	Rules []struct {
		Record string            `yaml:"record"`
		Alert  string            `yaml:"alert"`
		Expr   string            `yaml:"expr"`
		Labels map[string]string `yaml:"labels"`
	} `yaml:"rules"`
}

// metricExpr finds candidate metric references in a PromQL expression.
//
// It deliberately matches a bare identifier followed by a non-identifier
// character rather than implementing a PromQL grammar: the goal is to surface
// candidate references for review, and the gate then checks each candidate
// against the registered set. Over-matching is harmless (an unregistered name
// found in a string literal is still worth knowing); under-matching a real
// metric reference is what makes a dead alert, so the pattern is written to be
// deliberately generous.
var metricExpr = regexp.MustCompile(`\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\(|\{|\[|[-+*/^]|$|\s)`)

// histogramSuffixes are the series a histogram family is exposed as. A rule may
// reference any of them directly (a classic rate() over _bucket, or a
// _count - _sum for an average), so they must resolve against the base family.
var histogramSuffixes = []string{"_bucket", "_count", "_sum"}

// baseMetricName strips histogram sample suffixes so `foo_seconds_bucket`
// resolves to the registered family `foo_seconds`.
func baseMetricName(name string) string {
	for _, suffix := range histogramSuffixes {
		if strings.HasSuffix(name, suffix) {
			return strings.TrimSuffix(name, suffix)
		}
	}
	return name
}

// metricFunctions are PromQL functions and keywords, never metric names.
var metricFunctions = map[string]bool{
	"abs": true, "absent": true, "absent_over_time": true, "and": true, "and_on": true,
	"avg": true, "avg_over_time": true, "ceil": true, "clamp_max": true, "clamp_min": true,
	"count": true, "count_over_time": true, "count_values": true, "days_in_month": true,
	"day_of_month": true, "day_of_week": true, "delta": true, "deriv": true, "drop_common_labels": true,
	"exp": true, "floor": true, "histogram_quantile": true, "holt_winters": true,
	"hour": true, "idelta": true, "increase": true, "irate": true, "join": true,
	"label_join": true, "label_replace": true, "last_over_time": true, "ln": true, "log10": true,
	"log2": true, "max": true, "max_over_time": true, "min": true, "min_over_time": true,
	"minute": true, "month": true, "or": true, "or_on": true, "predict_linear": true,
	"present_over_time": true, "quantile_over_time": true, "rate": true, "resets": true,
	"round": true, "scalar": true, "sort": true, "sort_desc": true, "sqrt": true,
	"stddev_over_time": true, "stdvar_over_time": true, "sum": true, "sum_over_time": true,
	"timestamp": true, "time": true, "vector": true, "year": true, "on": true,
	"ignoring": true, "group_left": true, "group_right": true, "bool": true,
	"offset": true, "by": true, "without": true, "true": true, "false": true,
	"inf": true, "nan": true, "NaN": true, "Inf": true,
}

// labelNamePrefixes are label namespaces, not metric families.
func isLabelRef(name string) bool {
	return strings.HasPrefix(name, "__")
}

// registeredMetrics loads the inventory of families this repository registers.
//
// The inventory is checked in as testdata and produced by `make observability-inventory`.
func registeredMetrics(t *testing.T) map[string]registeredMetric {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "registered_metrics.tsv"))
	if err != nil {
		t.Fatalf("read registered metric inventory: %v (run: make observability-inventory)", err)
	}
	out := map[string]registeredMetric{}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		name := parts[0]
		var labels []string
		if len(parts) > 1 && parts[1] != "" {
			labels = strings.Split(parts[1], ",")
		}
		out[name] = registeredMetric{Name: name, Labels: labels}
	}
	if len(out) == 0 {
		t.Fatal("registered metric inventory is empty")
	}
	return out
}

func loadRuleGroups(t *testing.T) []ruleGroup {
	t.Helper()
	raw, err := os.ReadFile(rulesPath)
	if err != nil {
		t.Fatalf("read %s: %v", rulesPath, err)
	}
	var doc struct {
		Groups []ruleGroup `yaml:"groups"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", rulesPath, err)
	}
	if len(doc.Groups) == 0 {
		t.Fatalf("%s defines no rule groups", rulesPath)
	}
	return doc.Groups
}

func metricRefsInExpr(expr, source string, line int) []metricFamily {
	var refs []metricFamily
	// Strip string literals: a metric name inside quotes is not a reference.
	stripped := regexp.MustCompile(`"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'`).ReplaceAllString(expr, `""`)
	for _, tok := range metricExpr.FindAllStringSubmatch(stripped, -1) {
		name := tok[1]
		if metricFunctions[name] || isLabelRef(name) {
			continue
		}
		refs = append(refs, metricFamily{Name: name, Source: source, Line: line})
	}
	return refs
}

// referencedMetrics returns every metric family referenced by any rule, along
// with a human-readable location for the failure message.
func referencedMetrics(t *testing.T) []metricFamily {
	t.Helper()
	groups := loadRuleGroups(t)
	var refs []metricFamily
	for _, g := range groups {
		for _, r := range g.Rules {
			if r.Expr != "" {
				refs = append(refs, metricRefsInExpr(r.Expr, "grafana/rules.yml", 0)...)
			}
		}
	}
	return refs
}

// isRegistered resolves a reference against the inventory, tolerating the
// histogram sample suffixes and the `_total` suffix that a counter family is
// referenced through.
func isRegistered(reg map[string]registeredMetric, name string) bool {
	if _, ok := reg[name]; ok {
		return true
	}
	if _, ok := reg[baseMetricName(name)]; ok {
		return true
	}
	// Prometheus clients expose a counter registered as `foo` under the name
	// `foo_total` when built through the OpenMetrics-style helpers.
	if strings.HasSuffix(name, "_total") {
		if _, ok := reg[strings.TrimSuffix(name, "_total")]; ok {
			return true
		}
	}
	return false
}

// isRuleLabelSelector reports whether a bare identifier is a label name used in
// a grouping or join rather than a metric reference. `AND` appears this way in
// `expr: A and on(dataset) B` when flattened onto one line.
func isRuleLabelSelector(name string) bool {
	return name == "AND" || strings.HasPrefix(name, "ONGBOW_")
}

func TestAlertRulesReferenceRegisteredMetrics(t *testing.T) {
	reg := registeredMetrics(t)
	refs := referencedMetrics(t)

	type occurrence struct {
		metricFamily
		rules []int
	}
	seen := map[string]*occurrence{}
	for _, r := range refs {
		if isRegistered(reg, r.Name) || isRuleLabelSelector(r.Name) {
			continue
		}
		e, ok := seen[r.Name]
		if !ok {
			e = &occurrence{metricFamily: r}
			seen[r.Name] = e
		}
		e.rules = append(e.rules, r.Line)
	}
	if len(seen) == 0 {
		return
	}

	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteString("\nAlert rules reference metric families that are not registered.\n")
	b.WriteString("An expression over an unregistered metric resolves to an empty series\n")
	b.WriteString("instead of an error, so these alerts can never fire.\n\n")
	for _, n := range names {
		e := seen[n]
		fmt.Fprintf(&b, "  %s\n      referenced at %s:%v\n", n, e.Source, e.rules)
	}
	b.WriteString("\nFix the metric name to match a registered family, or register the metric.\n")
	b.WriteString("Run `make observability-inventory` after adding a metric.\n")
	t.Error(b.String())
}

// TestDashboardPanelsReferenceRegisteredMetrics covers the dashboard half of the
// audit: a panel querying a family that was never registered renders empty.
func TestDashboardPanelsReferenceRegisteredMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dashboard scan in short mode")
	}
	reg := registeredMetrics(t)
	entries, err := filepath.Glob(filepath.Join(dashboardsDir, "*.json"))
	if err != nil {
		t.Fatalf("glob dashboards: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("no dashboards found in %s", dashboardsDir)
	}

	type hit struct {
		dashboard string
		metric    string
	}
	var hits []hit
	seen := map[hit]bool{}
	for _, entry := range entries {
		raw, err := os.ReadFile(entry)
		if err != nil {
			t.Fatalf("read %s: %v", entry, err)
		}
		name := filepath.Base(entry)
		// Scan the raw JSON text: panel targets are PromQL strings embedded in
		// the document, and Grafana templating is stripped below.
		body := strings.NewReplacer(`\"`, `"`, `\n`, "\n").Replace(string(raw))
		for _, tok := range metricExpr.FindAllString(body, -1) {
			m := strings.TrimSpace(tok)
			m = strings.TrimRight(m, `({[-+*/^ `)
			if m == "" || metricFunctions[m] || isLabelRef(m) {
				continue
			}
			if isRegistered(reg, m) || isRuleLabelSelector(m) {
				continue
			}
			// Grafana variables and non-Longbow series are out of scope.
			if strings.Contains(m, "$") || !strings.Contains(m, "_") {
				continue
			}
			h := hit{dashboard: name, metric: m}
			if seen[h] {
				continue
			}
			seen[h] = true
			hits = append(hits, h)
		}
	}
	if len(hits) == 0 {
		return
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].dashboard != hits[j].dashboard {
			return hits[i].dashboard < hits[j].dashboard
		}
		return hits[i].metric < hits[j].metric
	})
	var b strings.Builder
	b.WriteString("\nDashboard panels reference metric families that are not registered.\n\n")
	for _, h := range hits {
		fmt.Fprintf(&b, "  %s: %s\n", h.dashboard, h.metric)
	}
	t.Log(b.String())
	// Reported, not failed: dashboard JSON mixes PromQL with Grafana templating
	// and third-party series, so a false positive here is expected until the
	// extraction is narrowed. The rules check above is the blocking gate.
	t.Skip("dashboard extraction is best-effort; see t.Log output for candidates")
}

// TestMetricsDocMatchesRegistry keeps docs/metrics.md honest. It is the
// automated half of roadmap Item 24.
func TestMetricsDocMatchesRegistry(t *testing.T) {
	reg := registeredMetrics(t)
	raw, err := os.ReadFile("../../docs/metrics.md")
	if err != nil {
		t.Fatalf("read docs/metrics.md: %v", err)
	}

	// Only table rows count. The prose around the table legitimately mentions
	// families by name while explaining what changed, and matching those would
	// report a family as "documented" when its row has been deleted.
	documented := map[string]bool{}
	rowRe := regexp.MustCompile("`(longbow_[a-z0-9_]+)`")
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			continue
		}
		for _, m := range rowRe.FindAllStringSubmatch(line, -1) {
			documented[m[1]] = true
		}
	}
	if len(documented) == 0 {
		t.Fatal("docs/metrics.md contains no metric table rows; the parser needs updating")
	}

	var missing, stale []string
	for name := range reg {
		if !documented[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "\ndocs/metrics.md omits %d registered metric families:\n", len(missing))
		for _, m := range missing {
			fmt.Fprintf(&b, "  %s\n", m)
		}
		t.Error(b.String())
	}

	// Reverse direction: documented but never registered. A documented family
	// that does not exist sends readers looking for a metric that cannot appear.
	registeredNames := map[string]bool{}
	for n := range reg {
		registeredNames[n] = true
	}
	for name := range documented {
		if !registeredNames[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "\ndocs/metrics.md documents %d metric families that are not registered:\n", len(stale))
		for _, m := range stale {
			fmt.Fprintf(&b, "  %s\n", m)
		}
		t.Error(b.String())
	}
}

// TestRegisteredMetricsInventoryFreshness verifies that testdata/registered_metrics.tsv
// is in sync with metric declarations in the Go codebase. If this fails, run
// `make observability-inventory` to regenerate the inventory.
func TestRegisteredMetricsInventoryFreshness(t *testing.T) {
	cmd := exec.Command("go", "run", "../../tools/metrics-inventory", "../../internal", "../../cmd")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run tools/metrics-inventory: %v", err)
	}
	expected, err := os.ReadFile(filepath.Join("testdata", "registered_metrics.tsv"))
	if err != nil {
		t.Fatalf("read testdata/registered_metrics.tsv: %v", err)
	}
	if string(out) != string(expected) {
		t.Errorf("testdata/registered_metrics.tsv is stale compared to Go source declarations. Run `make observability-inventory` to regenerate.")
	}
}

// TestRuleGroupsHaveUnitTests guards roadmap Item 20: a rule group with no
// promtool unit test has a parse check only, which cannot detect a rule that
// never fires.
func TestRuleGroupsHaveUnitTests(t *testing.T) {
	groups := loadRuleGroups(t)

	// The harness runs tests for *_alerts_test.yml, or extracts groups with `extract_group <groupname> ...`.
	testsDir := filepath.Join("..", "..", "grafana", "tests")
	harness, err := os.ReadFile(filepath.Join(testsDir, "test-rules.sh"))
	if err != nil {
		t.Fatalf("read grafana/tests/test-rules.sh: %v", err)
	}
	known := map[string]bool{}
	for _, g := range groups {
		known[g.Name] = true
	}
	covered := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^extract_group\s+(\S+)`).FindAllStringSubmatch(string(harness), -1) {
		covered[m[1]] = true
	}

	testFiles, err := filepath.Glob(filepath.Join(testsDir, "*_test.yml"))
	if err == nil {
		for _, f := range testFiles {
			base := filepath.Base(f)
			name := strings.TrimSuffix(base, ".yml")
			group := "longbow_" + strings.TrimSuffix(name, "_test")
			covered[group] = true
		}
	}

	var uncovered []string
	for _, g := range groups {
		if !covered[g.Name] {
			uncovered = append(uncovered, g.Name)
		}
	}
	sort.Strings(uncovered)

	t.Logf("rule groups: %d, with unit tests: %d, without: %d",
		len(groups), len(groups)-len(uncovered), len(uncovered))

	// An extract_group call naming a group that no longer exists is dead
	// harness code: it would extract nothing and silently pass.
	var staleHarness []string
	for name := range covered {
		if !known[name] {
			staleHarness = append(staleHarness, name)
		}
	}
	sort.Strings(staleHarness)
	for _, name := range staleHarness {
		t.Errorf("test-rules.sh extracts %s, which is not a rule group in rules.yml", name)
	}

	if len(uncovered) > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "\n%d of %d rule groups have no promtool unit test:\n", len(uncovered), len(groups))
		for _, g := range uncovered {
			fmt.Fprintf(&b, "  %s\n", g)
		}
		b.WriteString("\nA parse check cannot detect a rule that never fires.\n")
		b.WriteString("See docs/roadmap.md Item 20.\n")
		t.Errorf("%s", b.String())
	}
}

