#!/bin/bash

# Development utilities for Longbow
# Provides helpful tools for local development

set -e

# Function to show help
show_help() {
	echo "Longbow Development Utilities"
	echo ""
	echo "Usage: $0 [command] [options]"
	echo ""
	echo "Available commands:"
	echo "  help       Show this help message"
	echo "  start      Start Longbow in development mode"
	echo "  stop       Stop running Longbow processes"
	echo "  restart    Restart Longbow"
	echo "  status     Check status of Longbow processes"
	echo "  logs       Show Longbow logs"
	echo "  config     Show current configuration"
	echo "  bench      Run quick benchmark"
	echo "  profile     Profile performance"
	echo "  deps       Check dependencies"
	echo "  watch      Watch for file changes and restart"
	echo ""
	echo "Options:"
	echo "  --dev     Development mode (enables hot reload)"
	echo "  --debug    Enable debug logging"
	echo "  --port      Override port (default: 3000)"
}

# Start Longbow in development mode
start_dev() {
	local mode=""
	if [ "$1" = "--dev" ]; then
		mode=" with hot reload enabled"
	fi
	
	echo "Starting Longbow$mode..."
	
	# Set development environment variables
	export LONGBOW_LOG_LEVEL=debug
	export LONGBOW_HOT_RELOAD=true
	
	# Start with make
	if command -v make >/dev/null 2>&1; then
		make dev
	else
		# Fallback to direct go command
		go run ./cmd/longbow &
		echo $! > /tmp/longbow.pid
	fi
	
	echo "Longbow started"
}

# Stop running Longbow processes
stop_longbow() {
	echo "Stopping Longbow..."
	
	# Kill by PID file if exists
	if [ -f /tmp/longbow.pid ]; then
		pid=$(cat /tmp/longbow.pid)
		if kill -0 "$pid" 2>/dev/null; then
			echo "Stopped Longbow process $pid"
		else
			echo "Process $pid not running"
		fi
		rm -f /tmp/longbow.pid
	fi
	
	# Fallback: kill by process name
	pkill -f longbow || true
	
	echo "Longbow stopped"
}

# Restart Longbow
restart_longbow() {
	echo "Restarting Longbow..."
	stop_longbow
	sleep 2
	start_dev
}

# Check status
check_status() {
	echo "Checking Longbow status..."
	
	if [ -f /tmp/longbow.pid ]; then
		pid=$(cat /tmp/longbow.pid)
		if ps -p "$pid" > /dev/null; then
			echo "Longbow is running (PID: $pid)"
			echo "Data Server: http://localhost:3000"
			echo "Meta Server: http://localhost:3001"
			echo "Metrics: http://localhost:9090"
		else
			echo "Longbow is not running"
			rm -f /tmp/longbow.pid
		fi
	else
		echo "No PID file found"
		# Check if process is running by name
		if pgrep -f longbow > /dev/null; then
			echo "Longbow processes found but no PID file"
		else
			echo "Longbow is not running"
		fi
	fi
}

# Show logs
show_logs() {
	echo "Showing Longbow logs..."
	
	if command -v journalctl >/dev/null 2>&1; then
		journalctl -u longbow -f
	else
		# Fallback to log files
		if [ -d ./logs ]; then
			tail -f ./logs/longbow.log
		else
			echo "No log directory found"
		fi
	fi
}

# Show current configuration
show_config() {
	echo "Longbow Configuration:"
	echo "=================="
	
	# Show environment variables
	env | grep LONGBOW_ | sort
	
	# Show active config
	if command -v go >/dev/null 2>&1; then
		echo ""
		echo "Build Information:"
		go version
	fi
}

# Run quick benchmark
run_benchmark() {
	echo "Running quick benchmark..."
	
	# Build benchmark tool if needed
	if ! command -v bin/bench-tool >/dev/null 2>&1; then
		echo "Building benchmark tool..."
		make build
	fi
	
	# Run simple benchmark
	bin/bench-tool --mode=ingest --duration=10s --rows=1000
	bin/bench-tool --mode=search --duration=10s --queries=100
}

# Profile performance
profile_performance() {
	echo "Profiling Longbow performance..."
	
	# Check if pprof is available
	curl -s http://localhost:9090/debug/pprof/heap > /tmp/heap_profile.txt 2>/dev/null || echo "Failed to get heap profile"
	curl -s http://localhost:9090/debug/pprof/profile > /tmp/cpu_profile.txt 2>/dev/null || echo "Failed to get CPU profile"
	
	echo "Profiles saved to /tmp/"
	ls -la /tmp/*_profile.txt 2>/dev/null || true
}

# Check dependencies
check_deps() {
	echo "Checking Go dependencies..."
	
	# Check go.mod
	if [ -f go.mod ]; then
		echo "go.mod found"
		go mod verify
		echo "Dependencies are valid"
	else
		echo "No go.mod found"
	fi
	
	# Check for required tools
	echo ""
	echo "Checking required development tools..."
	
	tools=("make" "go" "docker" "git")
	for tool in "${tools[@]}"; do
		if command -v "$tool" >/dev/null 2>&1; then
			echo "✓ $tool"
		else
			echo "✗ $tool (missing)"
		fi
	done
}

# Watch for file changes and restart
watch_files() {
	echo "Watching for file changes..."
	
	if command -v fswatch >/dev/null 2>&1; then
		fswatch -o . -x "*.go" -c "make dev" &
		echo "Watching Go files for changes..."
	elif command -v inotifywait >/dev/null 2>&1; then
		inotifywait -r modify -e create,delete --format '%w %e' ./ | while read; do make dev; done &
		echo "Watching Go files with inotify..."
	else
		# Fallback to simple polling
		echo "File watching tools not available. Install fswatch or inotify-tools."
	fi
}

# Main script logic
case "$1" in
	help)
		show_help
		;;
	start)
		start_dev
		;;
	stop)
		stop_longbow
		;;
	restart)
		restart_longbow
		;;
	status)
		check_status
		;;
	logs)
		show_logs
		;;
	config)
		show_config
		;;
	bench)
		run_benchmark
		;;
	profile)
		profile_performance
		;;
	deps)
		check_deps
		;;
	watch)
		watch_files
		;;
	*)
		echo "Unknown command: $1"
		show_help
		exit 1
		;;
esac