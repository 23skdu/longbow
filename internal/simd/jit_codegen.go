package simd

import (
	"encoding/binary"
)

// WASM OpCodes
const (
	OpSIMDPrefix = 0xfd

	OpV128Load         = 0x00
	OpF32x4Add         = 0xe4
	OpF32x4Sub         = 0xe5
	OpF32x4Mul         = 0xe6
	OpF32x4ExtractLane = 0x1f

	OpBlock    = 0x02
	OpLoop     = 0x03
	OpBrIf     = 0x0d
	OpBr       = 0x0c
	OpEnd      = 0x0b
	OpLocalGet = 0x20
	OpLocalSet = 0x21
	OpLocalTee = 0x22
	OpF32Load  = 0x2a
	OpF32Store = 0x38
	OpI32Const = 0x41
	OpF32Const = 0x43
	OpI32Add   = 0x6a
	OpI32Mul   = 0x6c
	OpI32Shl   = 0x74
	OpI32GeU   = 0x4f
	OpI32GtU   = 0x4b
	OpF32Add   = 0x92
	OpF32Sub   = 0x93
	OpF32Mul   = 0x94
	OpF32Sqrt  = 0x91
)

type wasmGen struct {
	buf []byte
}

func (g *wasmGen) writeByte(b byte) {
	g.buf = append(g.buf, b)
}

func (g *wasmGen) writeU32(v uint32) {
	var buf [5]byte
	n := binary.PutUvarint(buf[:], uint64(v))
	g.buf = append(g.buf, buf[:n]...)
}

func (g *wasmGen) writeHeader() {
	g.buf = append(g.buf, 0x00, 0x61, 0x73, 0x6d)
	g.buf = append(g.buf, 0x01, 0x00, 0x00, 0x00)
}

func (g *wasmGen) writeSectionHeader(id byte, size int) {
	g.writeByte(id)
	g.writeU32(uint32(size))
}

// GenerateKernels generates a WASM module with both scalar and batch Euclidean kernels
func GenerateKernels(hasSIMD bool) []byte {
	g := &wasmGen{}
	g.writeHeader()

	// 1. Type Section
	// 0: (i32, i32, i32) -> f32 (scalar)
	// 1: (i32, i32, i32, i32, i32) -> void (batch)
	typeBuf := &wasmGen{}
	typeBuf.writeByte(0x02) // 2 types

	// Type 0
	typeBuf.writeByte(0x60)
	typeBuf.writeByte(0x03)
	typeBuf.writeByte(0x7f)
	typeBuf.writeByte(0x7f)
	typeBuf.writeByte(0x7f)
	typeBuf.writeByte(0x01)
	typeBuf.writeByte(0x7d)

	// Type 1
	typeBuf.writeByte(0x60)
	typeBuf.writeByte(0x05)
	typeBuf.writeByte(0x7f)
	typeBuf.writeByte(0x7f)
	typeBuf.writeByte(0x7f)
	typeBuf.writeByte(0x7f)
	typeBuf.writeByte(0x7f)
	typeBuf.writeByte(0x00)

	g.writeSectionHeader(1, len(typeBuf.buf))
	g.buf = append(g.buf, typeBuf.buf...)

	// 3. Function Section
	// Func 0: Type 0 (dist)
	// Func 1: Type 1 (dist_batch)
	funcBuf := &wasmGen{}
	funcBuf.writeByte(0x02) // 2 funcs
	funcBuf.writeByte(0x00)
	funcBuf.writeByte(0x01)

	g.writeSectionHeader(3, len(funcBuf.buf))
	g.buf = append(g.buf, funcBuf.buf...)

	// 5. Memory Section
	memBuf := &wasmGen{}
	memBuf.writeByte(0x01)
	memBuf.writeByte(0x00)
	memBuf.writeByte(0x01)
	g.writeSectionHeader(5, len(memBuf.buf))
	g.buf = append(g.buf, memBuf.buf...)

	// 7. Export Section
	// dist (func 0)
	// dist_batch (func 1)
	// memory (mem 0)
	expBuf := &wasmGen{}
	expBuf.writeByte(0x03) // 3 exports

	// dist
	str0 := "dist"
	expBuf.writeU32(uint32(len(str0)))
	expBuf.buf = append(expBuf.buf, str0...)
	expBuf.writeByte(0x00)
	expBuf.writeByte(0x00)

	// dist_batch
	str1 := "dist_batch"
	expBuf.writeU32(uint32(len(str1)))
	expBuf.buf = append(expBuf.buf, str1...)
	expBuf.writeByte(0x00)
	expBuf.writeByte(0x01)

	// memory
	str2 := "memory"
	expBuf.writeU32(uint32(len(str2)))
	expBuf.buf = append(expBuf.buf, str2...)
	expBuf.writeByte(0x02)
	expBuf.writeByte(0x00)

	g.writeSectionHeader(7, len(expBuf.buf))
	g.buf = append(g.buf, expBuf.buf...)

	// 10. Code Section
	codeGen := &wasmGen{}
	codeGen.writeByte(0x02) // 2 bodies

	// Body 0: dist (scalar)
	// Locals: 3:sum, 4:i, 5:temp
	// Params: 0,1,2
	b0 := GenerateScalarBody()
	codeGen.buf = append(codeGen.buf, b0...)

	// Body 1: dist_batch
	var b1 []byte
	if hasSIMD {
		b1 = GenerateBatchBodySIMD()
	} else {
		b1 = GenerateBatchBody()
	}
	codeGen.buf = append(codeGen.buf, b1...)

	g.writeSectionHeader(10, len(codeGen.buf))
	g.buf = append(g.buf, codeGen.buf...)

	return g.buf
}

func GenerateScalarBody() []byte {
	body := []byte{
		OpF32Const, 0, 0, 0, 0,
		OpLocalSet, 3,
		OpI32Const, 0,
		OpLocalSet, 4,
		OpBlock, 0x40,
		OpLoop, 0x40,
		OpLocalGet, 4,
		OpLocalGet, 2,
		OpI32GeU,
		OpBrIf, 1,

		OpLocalGet, 0,
		OpLocalGet, 4,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpF32Load, 0x02, 0x00,

		OpLocalGet, 1,
		OpLocalGet, 4,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpF32Load, 0x02, 0x00,

		OpF32Sub,
		OpLocalTee, 5,
		OpLocalGet, 5,
		OpF32Mul,
		OpLocalGet, 3,
		OpF32Add,
		OpLocalSet, 3,

		OpLocalGet, 4,
		OpI32Const, 1,
		OpI32Add,
		OpLocalSet, 4,
		OpBr, 0,
		OpEnd,
		OpEnd,
		OpLocalGet, 3,
		OpF32Sqrt,
		OpEnd,
	}
	locals := []byte{
		0x03,
		0x01, 0x7d, // sum
		0x01, 0x7f, // i
		0x01, 0x7d, // temp
	}

	buf := &wasmGen{}
	buf.writeU32(uint32(len(locals) + len(body)))
	buf.buf = append(buf.buf, locals...)
	buf.buf = append(buf.buf, body...)
	return buf.buf
}

func GenerateBatchBody() []byte {
	body := []byte{
		OpI32Const, 0,
		OpLocalSet, 5, // v_idx = 0

		OpBlock, 0x40,
		OpLoop, 0x40,
		OpLocalGet, 5,
		OpLocalGet, 2, // n_vecs
		OpI32GeU,
		OpBrIf, 1,

		// curr_vec_ptr = vecs_ptr + v_idx * dim * 4
		OpLocalGet, 1,
		OpLocalGet, 5,
		OpLocalGet, 3, // dim
		OpI32Mul,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpLocalSet, 9,

		OpF32Const, 0, 0, 0, 0,
		OpLocalSet, 7, // sum = 0.0
		OpI32Const, 0,
		OpLocalSet, 6, // d_idx = 0

		OpBlock, 0x40,
		OpLoop, 0x40,
		OpLocalGet, 6,
		OpLocalGet, 3, // dim
		OpI32GeU,
		OpBrIf, 1,

		// q[d_idx]
		OpLocalGet, 0,
		OpLocalGet, 6,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpF32Load, 0x02, 0x00,

		// v[d_idx]
		OpLocalGet, 9,
		OpLocalGet, 6,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpF32Load, 0x02, 0x00,

		OpF32Sub,
		OpLocalTee, 8,
		OpLocalGet, 8,
		OpF32Mul,
		OpLocalGet, 7,
		OpF32Add,
		OpLocalSet, 7,

		OpLocalGet, 6,
		OpI32Const, 1,
		OpI32Add,
		OpLocalSet, 6,
		OpBr, 0,
		OpEnd,
		OpEnd,

		// store result
		OpLocalGet, 4,
		OpLocalGet, 5,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpLocalGet, 7,
		OpF32Sqrt,
		OpF32Store, 0x02, 0x00,

		OpLocalGet, 5,
		OpI32Const, 1,
		OpI32Add,
		OpLocalSet, 5,
		OpBr, 0,
		OpEnd,
		OpEnd,
		OpEnd,
	}

	locals := []byte{
		0x05,
		0x01, 0x7f, // v_idx
		0x01, 0x7f, // d_idx
		0x01, 0x7d, // sum
		0x01, 0x7d, // temp
		0x01, 0x7f, // ptr
	}

	buf := &wasmGen{}
	buf.writeU32(uint32(len(locals) + len(body)))
	buf.buf = append(buf.buf, locals...)
	buf.buf = append(buf.buf, body...)
	return buf.buf
}

func GenerateBatchBodySIMD() []byte {
	// Locals:
	// 5: v_idx (i32)
	// 6: d_idx (i32)
	// 7: sum (f32)
	// 8: acc (v128)
	// 9: curr_vec_ptr (i32)
	// 10: temp_v128 (v128) - implied by tee/stack

	body := []byte{
		OpI32Const, 0,
		OpLocalSet, 5, // v_idx = 0

		OpBlock, 0x40,
		OpLoop, 0x40,
		OpLocalGet, 5,
		OpLocalGet, 2, // n_vecs
		OpI32GeU,
		OpBrIf, 1,

		// curr_vec_ptr = vecs_ptr + v_idx * dim * 4
		OpLocalGet, 1,
		OpLocalGet, 5,
		OpLocalGet, 3, // dim
		OpI32Mul,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpLocalSet, 9,

		// acc = zero
		OpSIMDPrefix, 0x0c, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		OpLocalSet, 8,

		OpI32Const, 0,
		OpLocalSet, 6, // d_idx = 0

		// Inner Loop (SIMD)
		OpBlock, 0x40,
		OpLoop, 0x40,

		// check if d_idx + 4 > dim
		OpLocalGet, 6,
		OpI32Const, 4,
		OpI32Add,
		OpLocalGet, 3,
		OpI32GtU,
		OpBrIf, 1, // break to scalar tail handling

		OpLocalGet, 0,
		OpLocalGet, 6,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpSIMDPrefix, OpV128Load, 0x02, 0x00,

		// db_vec = v128.load(curr_vec_ptr + d_idx * 4)
		OpLocalGet, 9,
		OpLocalGet, 6,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpSIMDPrefix, OpV128Load, 0x02, 0x00,

		OpSIMDPrefix, OpF32x4Sub,
		OpLocalTee, 10, // local.tee 10
		// Mul(Sub, Sub)
		OpLocalGet, 10,
		OpSIMDPrefix, OpF32x4Mul,
		OpLocalGet, 8,
		OpSIMDPrefix, OpF32x4Add,
		OpLocalSet, 8,

		OpLocalGet, 6,
		OpI32Const, 4,
		OpI32Add,
		OpLocalSet, 6,
		OpBr, 0,
		OpEnd,
		OpEnd,

		// Reduction of acc (v128) to sum (f32)
		OpF32Const, 0, 0, 0, 0,
		OpLocalSet, 7, // sum = 0

		// Lane 0
		OpLocalGet, 8,
		OpSIMDPrefix, OpF32x4ExtractLane, 0x00,
		OpLocalGet, 7,
		OpF32Add,
		OpLocalSet, 7,

		// Lane 1
		OpLocalGet, 8,
		OpSIMDPrefix, OpF32x4ExtractLane, 0x01,
		OpLocalGet, 7,
		OpF32Add,
		OpLocalSet, 7,

		// Lane 2
		OpLocalGet, 8,
		OpSIMDPrefix, OpF32x4ExtractLane, 0x02,
		OpLocalGet, 7,
		OpF32Add,
		OpLocalSet, 7,

		// Lane 3
		OpLocalGet, 8,
		OpSIMDPrefix, OpF32x4ExtractLane, 0x03,
		OpLocalGet, 7,
		OpF32Add,
		OpLocalSet, 7,

		// Scalar Tail Loop
		OpBlock, 0x40,
		OpLoop, 0x40,
		OpLocalGet, 6,
		OpLocalGet, 3,
		OpI32GeU,
		OpBrIf, 1,

		// q load
		OpLocalGet, 0,
		OpLocalGet, 6,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpF32Load, 0x02, 0x00,

		// db load
		OpLocalGet, 9,
		OpLocalGet, 6,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,
		OpF32Load, 0x02, 0x00,

		OpF32Sub,
		OpLocalSet, 11, // store diff
		OpLocalGet, 11,
		OpLocalGet, 11,
		OpF32Mul,
		OpLocalGet, 7, // sum
		OpF32Add,
		OpLocalSet, 7,

		OpLocalGet, 6,
		OpI32Const, 1,
		OpI32Add,
		OpLocalSet, 6,
		OpBr, 0,
		OpEnd,
		OpEnd,

		// Store Result
		OpLocalGet, 4,
		OpLocalGet, 5,
		OpI32Const, 2,
		OpI32Shl,
		OpI32Add,

		OpLocalGet, 7,
		OpF32Sqrt,
		OpF32Store, 0x02, 0x00,

		OpLocalGet, 5,
		OpI32Const, 1,
		OpI32Add,
		OpLocalSet, 5,
		OpBr, 0,
		OpEnd,
		OpEnd,
		OpEnd,
	}

	locals := []byte{
		0x07,
		0x01, 0x7f, // 5
		0x01, 0x7f, // 6
		0x01, 0x7d, // 7
		0x01, 0x7b, // 8
		0x01, 0x7f, // 9
		0x01, 0x7b, // 10
		0x01, 0x7d, // 11
	}

	buf := &wasmGen{}
	buf.writeU32(uint32(len(locals) + len(body)))
	buf.buf = append(buf.buf, locals...)
	buf.buf = append(buf.buf, body...)
	return buf.buf
}
