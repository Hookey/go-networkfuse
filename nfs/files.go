package nfs

import (
	"context"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// NewNFSCache creates a FileHandle out of a file descriptor. All
// operations are implemented. When using the Fd from a *os.File, call
// syscall.Dup() on the fd, to avoid os.File's finalizer from closing
// the file descriptor.
func NewNFSCache(fd int, os *openStat) fs.FileHandle {
	return &NFScache{fd: fd, os: os}
}

type NFScache struct {
	mu sync.Mutex
	fd int
	os *openStat
}

var _ = (fs.FileHandle)((*NFScache)(nil))
var _ = (fs.FileReader)((*NFScache)(nil))
var _ = (fs.FileWriter)((*NFScache)(nil))

func (f *NFScache) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	r := fuse.ReadResultFd(uintptr(f.fd), off, len(buf))
	t := time.Now()
	f.os.mu.Lock()
	f.setAtime(t)
	//f.os.read = true
	f.os.mu.Unlock()
	return r, fs.OK
}

func (f *NFScache) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := syscall.Pwrite(f.fd, data, off)
	if err == nil {
		sz := int64(n) + off
		if sz > f.os.st.Size {
			f.setSize(sz)
		}
		t := time.Now()
		f.setMtime(t)
		//f.os.write = true
	}
	return uint32(n), fs.ToErrno(err)
}

func (f *NFScache) Allocate(ctx context.Context, off uint64, n uint64, mode uint32) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	n64 := int64(n)
	off64 := int64(off)
	err := syscall.Fallocate(f.fd, mode, off64, n64)
	if err == nil {
		t := time.Now()
		sz := off64 + n64
		if sz > f.os.st.Size {
			f.setSize(sz)
			//f.os.write = true
			f.setMtime(t)
			f.setCtime(t)
		} else {
			f.setCtime(t)
		}
	}

	return fs.ToErrno(err)
}

var _ = (fs.FileLseeker)((*NFScache)(nil))

func (f *NFScache) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := unix.Seek(f.fd, int64(off), int(whence))
	return uint64(n), fs.ToErrno(err)
}

func (f *NFScache) Close() syscall.Errno {
	if f.fd != -1 {
		err := syscall.Close(f.fd)
		f.fd = -1
		return fs.ToErrno(err)
	}
	return syscall.EBADF
}

func (f *NFScache) setAttr(in *fuse.SetAttrIn) syscall.Errno {
	f.os.mu.Lock()
	defer f.os.mu.Unlock()

	mode, ok := in.GetMode()
	if ok {
		f.setMode(mode)
	}

	uid32, uok := in.GetUID()
	if uok {
		f.setUid(uid32)
	}

	gid32, gok := in.GetGID()
	if gok {
		f.setGid(gid32)
	}

	mtime, mok := in.GetMTime()
	if mok {
		f.setMtime(mtime)
	}

	atime, aok := in.GetATime()
	if aok {
		f.setAtime(atime)
	}

	now := time.Now()
	sz, sok := in.GetSize()
	if sok {
		sz64 := int64(sz)
		errno := fs.ToErrno(syscall.Ftruncate(f.fd, sz64))
		if errno != 0 {
			f.os.change = false
			return errno
		}
		f.setSize(sz64)
		f.setMtime(now)
		//c.os.write = true
	}

	f.setCtime(now)
	return 0
}

func (f *NFScache) setMode(m uint32) {
	f.os.st.Mode = m
	f.os.change = true
}

func (f *NFScache) setUid(u uint32) {
	f.os.st.Uid = u
	f.os.change = true
}

func (f *NFScache) setGid(g uint32) {
	f.os.st.Gid = g
	f.os.change = true
}

func (f *NFScache) setSize(sz int64) {
	f.os.st.Size = sz
	f.os.st.Blocks = blocksFrom(sz)
	f.os.write = true
	f.os.change = true
}

func blocksFrom(size int64) int64 {
	return ((4095 + size) >> 12) << 3
}

func (f *NFScache) setMtime(t time.Time) {
	f.os.st.Mtim.Sec = t.Unix()
	f.os.st.Mtim.Nsec = int64(t.Nanosecond())
	f.os.change = true
}

func (f *NFScache) setAtime(t time.Time) {
	f.os.st.Atim.Sec = t.Unix()
	f.os.st.Atim.Nsec = int64(t.Nanosecond())
	f.os.change = true
}

func (f *NFScache) setCtime(t time.Time) {
	f.os.st.Ctim.Sec = t.Unix()
	f.os.st.Ctim.Nsec = int64(t.Nanosecond())
	f.os.change = true
}

func (f *NFScache) getAttr() *syscall.Stat_t {
	return &f.os.st
}

/*
var _ = (FileGetlker)((*nfsCache)(nil))
var _ = (FileSetlker)((*nfsCache)(nil))
var _ = (FileSetlkwer)((*nfsCache)(nil))
var _ = (FileSetattrer)((*nfsCache)(nil))
var _ = (FileAllocater)((*nfsCache)(nil))

const (
	_OFD_GETLK  = 36
	_OFD_SETLK  = 37
	_OFD_SETLKW = 38
)

func (f *nfsCache) Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) (errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	flk := syscall.Flock_t{}
	lk.ToFlockT(&flk)
	errno = ToErrno(syscall.FcntlFlock(uintptr(f.fd), _OFD_GETLK, &flk))
	out.FromFlockT(&flk)
	return
}

func (f *nfsCache) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	return f.setLock(ctx, owner, lk, flags, false)
}

func (f *nfsCache) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	return f.setLock(ctx, owner, lk, flags, true)
}

func (f *nfsCache) setLock(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, blocking bool) (errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if (flags & fuse.FUSE_LK_FLOCK) != 0 {
		var op int
		switch lk.Typ {
		case syscall.F_RDLCK:
			op = syscall.LOCK_SH
		case syscall.F_WRLCK:
			op = syscall.LOCK_EX
		case syscall.F_UNLCK:
			op = syscall.LOCK_UN
		default:
			return syscall.EINVAL
		}
		if !blocking {
			op |= syscall.LOCK_NB
		}
		return ToErrno(syscall.Flock(f.fd, op))
	} else {
		flk := syscall.Flock_t{}
		lk.ToFlockT(&flk)
		var op int
		if blocking {
			op = _OFD_SETLKW
		} else {
			op = _OFD_SETLK
		}
		return ToErrno(syscall.FcntlFlock(uintptr(f.fd), op, &flk))
	}
}

*/
