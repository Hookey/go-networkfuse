package nfs

import (
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type NFSDirStream struct {
	// Protects against clean buf
	mu     sync.Mutex
	ino    uint64
	offset int

	//TODO: shared among readdir
	buf []*Item
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

	return len(ds.buf) > ds.offset
}

func (ds *NFSDirStream) Next() (fuse.DirEntry, syscall.Errno) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	i := ds.buf[ds.offset]

	result := fuse.DirEntry{
		Ino:  i.Ino,
		Mode: i.Stat.Mode,
		Name: i.Link.Name,
	}
	ds.offset++

	return result, fs.OK
}
