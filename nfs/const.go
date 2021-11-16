package nfs

import "math"

const (
	RecycleBin = 0
	TempBin    = math.MaxUint64 - 1
	RootBin    = math.MaxUint64
)
