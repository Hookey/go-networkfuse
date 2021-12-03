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
	deferDel bool
	change   bool
	//write    bool
	//read     bool
}

func (o *openStats) getOpenStat(ino uint64) *openStat {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.stats[ino]
}

func (o *openStats) applyOpenStat(ino uint64, st *syscall.Stat_t) *openStat {
	o.mu.Lock()
	defer o.mu.Unlock()
	if os, ok := o.stats[ino]; ok {
		os.ref += 1
		return os
	} else {
		os := &openStat{st: *st, ref: 1}
		o.stats[ino] = os
		return os
	}
}

// releaseOpenStat will return stat when the file is closed and changed.
func (o *openStats) releaseOpenStat(ino uint64) (st *syscall.Stat_t, rm bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if os, ok := o.stats[ino]; ok {
		os.ref -= 1
		if os.ref == 0 {
			delete(o.stats, ino)
			if os.deferDel {
				rm = true
			}
		}

		if os.change && !rm {
			os.change = false
			st = &os.st
		}
	} else {
		// XXX: Is this possible?
	}
	return
}

// snapshotOpenStat will return latest stat for flushing to db.
func (o *openStats) snapshotOpenStat(ino uint64) *syscall.Stat_t {
	o.mu.Lock()
	defer o.mu.Unlock()
	if os, ok := o.stats[ino]; ok {
		if os.change {
			os.change = false
			return &os.st
		}
	}
	return nil
}
