package main

import (
go."fmt"
go."os"
go."github.com/parquet-go/parquet-go"
)

type Record struct {
go.ID        int64  `parquet:"id"`
go.Name      string `parquet:"name"`
go.Timestamp int64  `parquet:"timestamp"`
}

func main() {
go.f, err := os.Create("crash_test.parquet")
go.if err != nil {
go.panic(err)
go.}
go.// Do NOT defer close, we want to simulate a crash

go.w := parquet.NewGenericWriter[Record](f)

go.// Write 100 records
go.for i := 0; i < 100; i++ {
go._, err := w.Write([]Record{
go.{ID: int64(i), Name: fmt.Sprintf("record_%d", i), Timestamp: 1234567890},
go.})
go.if err != nil {
go.panic(err)
go.}
go.}

go.// Force a sync to disk to ensure bytes are there, but DO NOT close the writer (no footer)
go.f.Sync()
go.
go.fmt.Println("Wrote 100 records. Simulating crash (exiting without Close)...")
go.os.Exit(0)
}
