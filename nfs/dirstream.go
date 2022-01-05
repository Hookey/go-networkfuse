package nfs

import (
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type NFSDirStream struct {
	// Protects against clean buf
	mu  sync.Mutex
	ino uint64

	//TODO: shared among readdir
	buf []*Item
	// buf offset
	boffset int
	// dentry offset in buf[i]
	doffset int
}

func NewNFSDirStream(self *fs.Inode, readdir func(self *fs.Inode) []*Item) (fs.DirStream, syscall.Errno) {
	ino := self.StableAttr().Ino
	ds := &NFSDirStream{
		ino: ino,
	}

	ds.buf = readdir(self)
	return ds, fs.OK
}

func (ds *NFSDirStream) Close() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.buf = nil
}

func (ds *NFSDirStream) HasNext() bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for ; ds.boffset < len(ds.buf); ds.boffset++ {
		i := ds.buf[ds.boffset]

		// These are "." and ".."
		if ds.boffset < 2 {
			return true
		}

		for ; ds.doffset < len(i.Name); ds.doffset++ {
			if i.Pino[ds.doffset] == ds.ino {
				return true
			}
		}
		ds.doffset = 0
	}

	return false
}

func (ds *NFSDirStream) Next() (fuse.DirEntry, syscall.Errno) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	i := ds.buf[ds.boffset]
	logger.Infof("%v, %d, %d", i.Ino, len(ds.buf), len(i.Name))

	result := fuse.DirEntry{
		Ino:  i.Ino,
		Mode: i.Stat.Mode,
		Name: i.Name[ds.doffset],
	}

	// These are "." and ".."
	if ds.boffset < 2 {
		ds.boffset++
	} else {
		ds.doffset++
	}

	return result, fs.OK
}
