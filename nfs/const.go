package nfs

import "math"

const (
	RecycleBin uint64 = iota + math.MaxUint64 - 2
	ReclaimBin
	RootBin
)

// Detailed flag for file state, states are stored in Stat_t.X__unused[1]
const (
	Recycled int64 = iota
	Reclaiming
	Used
	Uploading
	Downloading
	Unused
	Archived
)

// Rename related flags
const (
	RENAME int = iota
	REPLACE
	REPLACE_OPEN
	EXCHANGE
)
