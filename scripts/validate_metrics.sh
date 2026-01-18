#!/bin/bash
# Longbow Metrics Validation Script
# Validates metrics across cluster nodes and analyzes dashboard coverage

set -e

METRICS_FILE="internal/metrics/metrics.go"
DASHBOARD_FILE="grafana/dashboards/longbow.json"
CLUSTER_PORTS="9090 9091 9092"

echo "=================================="
echo "📋 LONGBOW METRICS VALIDATION"
echo "=================================="

# Extract defined metrics
echo ""
echo "🔍 Extracting defined metrics from metrics.go..."
DEFINED_METRICS=$(grep -oE 'Name:\s+"longbow_[^"]+' "$METRICS_FILE" | sed 's/Name: "//g' | sort -u)
DEFINED_COUNT=$(echo "$DEFINED_METRICS" | wc -l | tr -d ' ')
echo "   Found $DEFINED_COUNT defined metrics"

# Scrape cluster metrics
echo ""
echo "📊 Scraping cluster metrics..."
TEMP_METRICS="/tmp/longbow_cluster_metrics.txt"
echo > "$TEMP_METRICS"

for PORT in $CLUSTER_PORTS; do
    echo "   - Scraping http://localhost:$PORT/metrics..."
    curl -s "http://localhost:$PORT/metrics" | grep "^longbow_" >> "$TEMP_METRICS" 2>/dev/null || true
done

EMITTING_METRICS=$(cat "$TEMP_METRICS" | cut -d'{' -f1 | cut -d' ' -f1 | sort -u)
EMITTING_COUNT=$(echo "$EMITTING_METRICS" | wc -l | tr -d ' ')
echo "   Found $EMITTING_COUNT emitting metrics across cluster"

# Count non-zero metrics
NON_ZERO_COUNT=$(cat "$TEMP_METRICS" | awk '{print $NF}' | grep -v "^0$" | wc -l | tr -d ' ')
echo "   Found $NON_ZERO_COUNT non-zero metric values"

# Extract dashboard metrics
echo ""
echo "📈 Extracting dashboard metrics..."
if [ -f "$DASHBOARD_FILE" ]; then
    DASHBOARD_METRICS=$(grep -oE 'longbow_[a-z_0-9]+' "$DASHBOARD_FILE" | sort -u)
    DASHBOARD_COUNT=$(echo "$DASHBOARD_METRICS" | wc -l | tr -d ' ')
    echo "   Found $DASHBOARD_COUNT unique metrics in dashboard"
else
    echo "   ⚠️  Dashboard file not found"
    DASHBOARD_METRICS=""
    DASHBOARD_COUNT=0
fi

# Calculate coverage
if [ "$DEFINED_COUNT" -gt 0 ]; then
    COVERAGE=$(echo "scale=1; $DASHBOARD_COUNT * 100 / $DEFINED_COUNT" | bc)
else
    COVERAGE=0
fi

echo ""
echo "=================================="
echo "📊 SUMMARY"
echo "=================================="
echo "✅ Total Defined Metrics: $DEFINED_COUNT"
echo "📡 Total Emitting Metrics: $EMITTING_COUNT"
echo "💫 Total Non-Zero Values: $NON_ZERO_COUNT"
echo "📊 Total Dashboard Metrics: $DASHBOARD_COUNT"
echo "📈 Dashboard Coverage: ${COVERAGE}%"

# Find metrics not in dashboard
echo ""
echo "=================================="
echo "📉 TOP 50 METRICS NOT IN DASHBOARD"
echo "=================================="
comm -23 <(echo "$DEFINED_METRICS" | sort) <(echo "$DASHBOARD_METRICS" | sort) | head -50

# Find metrics not emitting
echo ""
echo "===================================="
echo "⚠️  METRICS NOT EMITTING (Top 30)"
echo "===================================="
comm -23 <(echo "$DEFINED_METRICS" | sort) <(echo "$EMITTING_METRICS" | sort) | head -30

# Cleanup
rm -f "$TEMP_METRICS"

echo ""
echo "✅ Validation complete"
