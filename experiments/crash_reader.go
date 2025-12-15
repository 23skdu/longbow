package main

import (
"fmt"
"os"
"github.com/parquet-go/parquet-go"
)

type Record struct {
ID        int64  `parquet:"id"`
Name      string `parquet:"name"`
Timestamp int64  `parquet:"timestamp"`
}

func main() {
f, err := os.Open("crash_test.parquet")
if err != nil {
panic(err)
}
defer f.Close()

stat, _ := f.Stat()
fmt.Printf("File size: %d bytes\n", stat.Size())

// Try to read
rows, err := parquet.ReadFile[Record]("crash_test.parquet")
if err != nil {
fmt.Printf("Read Failed: %v\n", err)
} else {
fmt.Printf("Read Success! Found %d rows\n", len(rows))
}
}
