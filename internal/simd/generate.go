package simd

//go:generate go run gen/softmax_gen.go -out softmax_avx512_amd64.s -pkg simd
//go:generate go run gen/all_kernels_gen.go -out all_kernels_avo_amd64.s -pkg simd
