//go:build !gpu || !linux || !cgo

package cuda

import (
	"testing"
)

// TestContractCUDAStub runs the test contract cuda stub operation.
func TestContractCUDAStub(t *testing.T) {
	result := ContractCUDA(nil, nil, nil, false, 0, 0, 0)
	if result != false {
		t.Errorf("ContractCUDA stub should return false, got %v", result)
	}
}

// TestContractCADMAWithArgs runs the test contract cadma with args operation.
func TestContractCADMAWithArgs(t *testing.T) {
	result := ContractCUDA([]byte{1, 2, 3}, []byte{4, 5, 6}, []byte{7, 8, 9}, true, 10, 20, 30)
	if result != false {
		t.Errorf("ContractCUDA stub should return false, got %v", result)
	}
}
