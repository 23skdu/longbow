package simd

// Only all_kernels_avo_amd64.s is generated.
//
// softmax_avx512_amd64.s is hand-maintained. gen/softmax_gen.go still exists
// but is stale: it emits a much smaller softmaxAVX512Kernel than the committed
// file, with a reduced-order exp polynomial evaluated in ascending order
// instead of the committed degree-5 Horner form. Running it would silently
// downgrade the kernel, so it is deliberately not wired up here. Porting the
// committed kernel into Avo needs an AVX-512 host to validate; see
// docs/roadmap.md.
//
//go:generate go run gen/all_kernels_gen.go -out all_kernels_avo_amd64.s -pkg simd
