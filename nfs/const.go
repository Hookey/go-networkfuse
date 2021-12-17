package nfs

import "math"

const (
	RecycleBin uint64 = iota + math.MaxUint64 - 2
	ReclaimBin
	RootBin
)

const (
	Used int = iota
	Reclaiming
	Recycled
)

// Rename related flags
const (
	RENAME int = iota
	REPLACE
	REPLACE_OPEN
	EXCHANGE
)
