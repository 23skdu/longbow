#define VFADD_V(m, n, d) WORD $(0x4e20d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFSUB_V(m, n, d) WORD $(0x4ea0d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFMUL_V(m, n, d) WORD $(0x6e20dc00 | ((m) << 16) | ((n) << 5) | (d))
#define VFMLA_V(m, n, d) WORD $(0x4e20cc00 | ((m) << 16) | ((n) << 5) | (d))
#define VFNEG_V(n, d)    WORD $(0x6ea1f800 | ((n) << 5) | (d))
#define VFCVTZS_V(n, d)  WORD $(0x4e21b800 | ((n) << 5) | (d))
#define VSCVTF_V(n, d)   WORD $(0x4e21d800 | ((n) << 5) | (d))
#define VFRINTM_V(n, d)  WORD $(0x4e214000 | ((n) << 5) | (d))
#define VFRECPE_V(n, d)  WORD $(0x4e21d000 | ((n) << 5) | (d))
#define VFRECPS_V(m, n, d) WORD $(0x4e20fc00 | ((m) << 16) | ((n) << 5) | (d))

// Float16 multiply-accumulate long (FMLAL / FMLAL2)
// Vd.4S += Vn.4H * Vm.4H (widening: half → single)
// Non-standard encoding: bits[4:0]=Vd, [9:5]=Vn, [20:16]=Vm
#define VFMLAL_V(m, n, d)  WORD $(0x4E20EC00 | ((m) << 16) | ((n) << 5) | (d))
#define VFMLAL2_V(m, n, d) WORD $(0x6E20CC00 | ((m) << 16) | ((n) << 5) | (d))

// Float16 multiply-subtract long (FMLSL / FMLSL2)
// Vd.4S += -(Vn.4H * Vm.4H)
#define VFMLSL_V(m, n, d)  WORD $(0x4EA0EC00 | ((m) << 16) | ((n) << 5) | (d))
#define VFMLSL2_V(m, n, d) WORD $(0x6EA0CC00 | ((m) << 16) | ((n) << 5) | (d))

// Integer NEON macros for int16/int8 SIMD kernels
#define VSSHLL_4S(n, d)   WORD $(0x0F10A400 | ((n) << 5) | (d))
#define VSSHLL2_4S(n, d)  WORD $(0x4F10A400 | ((n) << 5) | (d))
#define VSUB_4S(m, n, d)  WORD $(0x6EA08400 | ((m) << 16) | ((n) << 5) | (d))
#define VMUL_4S(m, n, d)  WORD $(0x4EA09C00 | ((m) << 16) | ((n) << 5) | (d))
#define VSADDLP_2D(n, d)  WORD $(0x4EA02800 | ((n) << 5) | (d))
#define VUADDLP_2D(n, d)  WORD $(0x6EA02800 | ((n) << 5) | (d))
#define VSSHLL_8H(n, d)   WORD $(0x0F08A400 | ((n) << 5) | (d))
#define VSSHLL2_8H(n, d)  WORD $(0x4F08A400 | ((n) << 5) | (d))
#define VSUB_8H(m, n, d)  WORD $(0x6E608400 | ((m) << 16) | ((n) << 5) | (d))
#define VMUL_8H(m, n, d)  WORD $(0x4E609C00 | ((m) << 16) | ((n) << 5) | (d))
#define VADD_2D(m, n, d)  WORD $(0x4EE08400 | ((m) << 16) | ((n) << 5) | (d))

// Float64 (D2) variants for FADD and FSUB — the existing VFADD_V/VFSUB_V encode .4S
// FADD .2D: bit 22=1 (vs bit 22=0 for .4S); FSUB .2D: bit 22=1 AND bit 23=1
#define VFADD_D(m, n, d) WORD $(0x4e60d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFSUB_D(m, n, d) WORD $(0x4ee0d400 | ((m) << 16) | ((n) << 5) | (d))

// Bitmask and zero-extend for uint8 ops
#define VBIC_8B(m, n, d) WORD $(0x0E401C00 | ((m) << 16) | ((n) << 5) | (d))
#define VMOVI_8B(n, d)   WORD $(0x0F00E400 | ((n) << 5) | (d))

// Unsigned integer NEON macros for uint8 SIMD kernels (UABDL, UMLAL, etc.)
// Register encoding convention: bits[20:16]=Vm via (m<<16), [9:5]=Vn via (n<<5), [4:0]=Vd via (d)
// UABDL Vd.8H, Vn.8B, Vm.8B: unsigned absolute difference, long (8B→8H)
#define VUABDL_8H(m, n, d)  WORD $(0x2E207000 | ((m) << 16) | ((n) << 5) | (d))
// UMLAL Vd.4S, Vn.4H, Vm.4H: unsigned multiply-accumulate long (lower 4)
#define VUMLAL_4S(m, n, d)  WORD $(0x2E608000 | ((m) << 16) | ((n) << 5) | (d))
// UMLAL2 Vd.4S, Vn.8H, Vm.8H: unsigned multiply-accumulate long (upper 4)
#define VUMLAL2_4S(m, n, d) WORD $(0x6E608000 | ((m) << 16) | ((n) << 5) | (d))
// UMULL Vd.8H, Vn.8B, Vm.8B: unsigned multiply long (8B→8H)
#define VUMULL_8H(m, n, d)  WORD $(0x2E20C000 | ((m) << 16) | ((n) << 5) | (d))
// UADDW Vd.4S, Vn.4S, Vm.4H: unsigned add wide (32-bit += 16-bit, lower 4)
#define VUADDW_4S(m, n, d)  WORD $(0x2E601000 | ((m) << 16) | ((n) << 5) | (d))
// UADDW2 Vd.4S, Vn.4S, Vm.8H: unsigned add wide (upper 4)
#define VUADDW2_4S(m, n, d) WORD $(0x6E601000 | ((m) << 16) | ((n) << 5) | (d))

