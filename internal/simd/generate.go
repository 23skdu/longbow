package simd

// Only kernels_*_amd64.s are generated.
//
// softmax_avx512_amd64.s is hand-maintained. gen/softmax_gen.go still exists
// but is stale: it emits a much smaller softmaxAVX512Kernel than the committed
// file, with a reduced-order exp polynomial evaluated in ascending order
// instead of the committed degree-5 Horner form. Running it would silently
// downgrade the kernel, so it is deliberately not wired up here. Porting the
// committed kernel into Avo needs an AVX-512 host to validate; see
// docs/development/roadmap.md.
//
//go:generate go run gen/all_kernels_gen.go -family float32 -out kernels_float32_amd64.s -pkg simd
//go:generate go run gen/all_kernels_gen.go -family vertical -out kernels_vertical_amd64.s -pkg simd
//go:generate go run gen/all_kernels_gen.go -family float64 -out kernels_float64_amd64.s -pkg simd
//go:generate go run gen/all_kernels_gen.go -family f16 -out kernels_f16_amd64.s -pkg simd
//go:generate go run gen/all_kernels_gen.go -family reductions -out kernels_reductions_amd64.s -pkg simd
