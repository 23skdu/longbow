//go:build js && wasm

package main

import (
	"github.com/23skdu/longbow/pkg/wasmclient"
)

func main() {
	wasmclient.RegisterJSGlobals()
	select {}
}
