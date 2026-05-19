#include "textflag.h"

// func pause()
TEXT ·pause(SB), NOSPLIT, $0-0
	YIELD
	RET
