#include "textflag.h"

// func storeFence()
TEXT ·storeFence(SB), NOSPLIT|NOFRAME, $0-0
	DMB $0xa // ISHST: earlier stores before later stores
	RET

// func loadFence()
TEXT ·loadFence(SB), NOSPLIT|NOFRAME, $0-0
	DMB $0x9 // ISHLD: earlier loads before later loads and stores
	RET
