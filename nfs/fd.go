package nfs

import (
	"sync"
	"syscall"
)

type openStats struct {
	mu    sync.Mutex
	stats map[uint64]*openStat
}

type openStat struct {
	mu       sync.Mutex
	st       syscall.Stat_t
	ref      int32
	archive  bool
	deferDel bool
	change   bool
	//write    bool
	//read     bool
}

func (os *openStats) getOpenStat(ino uint64) *openStat {
	os.mu.Lock()
	defer os.mu.Unlock()
	return os.stats[ino]
}

func (os *openStats) applyOpenStat(ino uint64, st *syscall.Stat_t) *openStat {
	os.mu.Lock()
	defer os.mu.Unlock()
	if o, ok := os.stats[ino]; ok {
		o.ref += 1
		return o
	} else {
		o := &openStat{st: *st, ref: 1}
		os.stats[ino] = o
		return o
	}
}

// releaseOpenStat will return stat when the file is closed and changed.
func (os *openStats) releaseOpenStat(ino uint64) (st *syscall.Stat_t, rm bool, kicked bool) {
	os.mu.Lock()
	defer os.mu.Unlock()
	if o, ok := os.stats[ino]; ok {
		o.ref -= 1
		if o.ref == 0 {
			delete(os.stats, ino)
			if o.deferDel {
				rm = true
			} else if o.archive {
				kicked = true
			}
		}

		// Reset change flag, and will sync st to db
		if o.change && !rm {
			o.change = false
			st = &o.st
		}
	} else {
		// XXX: Is this possible?
	}
	return
}

// snapshotOpenStat will return latest stat for flushing to db.
func (os *openStats) snapshotOpenStat(ino uint64) *syscall.Stat_t {
	os.mu.Lock()
	defer os.mu.Unlock()
	if o, ok := os.stats[ino]; ok {
		if o.change {
			o.change = false
			return &o.st
		}
	}
	return nil
}

func (o *openStat) Setstate(s int64) {
	o.st.X__unused[1] = s
}

func (o *openStat) Getstate() int64 {
	return o.st.X__unused[1]
}
