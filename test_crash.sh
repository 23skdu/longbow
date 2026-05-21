export LONGBOW_MAX_MEMORY=18000000000
./bin/longbow > server_crash.log 2>&1 &
SERVER_PID=$!
sleep 2
./bin/bench-tool -mode vec -uri grpc://127.0.0.1:3000 -dim 768 -dtype int32 -scale 5000 -queries 100 -workers 8 -dataset test_crash_dataset -search-modes all
kill -9 $SERVER_PID
cat server_crash.log | tail -n 100
