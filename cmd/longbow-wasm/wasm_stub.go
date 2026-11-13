//go:build !(js && wasm)

package main

import "fmt"

func main() {
	fmt.Println("Build with GOOS=js GOARCH=wasm go build -o longbow.wasm ./cmd/longbow-wasm")
}
