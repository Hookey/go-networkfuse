package nfs

import "math"

const (
	RecycleBin uint64 = iota + math.MaxUint64 - 2
	TempBin
	RootBin
)

const (
	Used int = iota
	Reclaiming
	Reclaimed
)

const (
	RENAME int = iota
	REPLACE
	REPLACE_OPEN
	EXCHANGE
)
