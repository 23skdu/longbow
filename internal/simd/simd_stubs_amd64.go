//go:build !amd64

package simd

import (
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func int8ToFloat32AVX2(src []int8, dst []float32) { int8ToFloat32Generic(src, dst) }
func uint8ToFloat32AVX2(src []uint8, dst []float32) { uint8ToFloat32Generic(src, dst) }
func int16ToFloat32AVX2(src []int16, dst []float32) { int16ToFloat32Generic(src, dst) }
func uint16ToFloat32AVX2(src []uint16, dst []float32) { uint16ToFloat32Generic(src, dst) }
func int32ToFloat32AVX2(src []int32, dst []float32) { int32ToFloat32Generic(src, dst) }
func uint32ToFloat32AVX2(src []uint32, dst []float32) { uint32ToFloat32Generic(src, dst) }
func float16ToFloat32AVX2(src []float16.Num, dst []float32) { float16ToFloat32Generic(src, dst) }

func int8ToFloat32AVX512(src []int8, dst []float32) { int8ToFloat32Generic(src, dst) }
func uint8ToFloat32AVX512(src []uint8, dst []float32) { uint8ToFloat32Generic(src, dst) }
func int16ToFloat32AVX512(src []int16, dst []float32) { int16ToFloat32Generic(src, dst) }
func uint16ToFloat32AVX512(src []uint16, dst []float32) { uint16ToFloat32Generic(src, dst) }
func int32ToFloat32AVX512(src []int32, dst []float32) { int32ToFloat32Generic(src, dst) }
func uint32ToFloat32AVX512(src []uint32, dst []float32) { uint32ToFloat32Generic(src, dst) }
func float16ToFloat32AVX512(src []float16.Num, dst []float32) { float16ToFloat32Generic(src, dst) }
