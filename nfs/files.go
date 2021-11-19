package nfs

import (
	"context"
	"sync"

	//	"time"

	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// NewNFSCache creates a FileHandle out of a file descriptor. All
// operations are implemented. When using the Fd from a *os.File, call
// syscall.Dup() on the fd, to avoid os.File's finalizer from closing
// the file descriptor.
func NewNFSCache(fd int, st *syscall.Stat_t) fs.FileHandle {
	return &NFScache{fd: fd, st: *st}
}

type NFScache struct {
	mu sync.Mutex
	fd int
	// TODO: shared data between openfiles to the same file
	// in loopback fs, can rely on fd.
	st    syscall.Stat_t
	write bool
	read  bool
}

var _ = (fs.FileHandle)((*NFScache)(nil))
var _ = (fs.FileReader)((*NFScache)(nil))
var _ = (fs.FileWriter)((*NFScache)(nil))

func (f *NFScache) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := fuse.ReadResultFd(uintptr(f.fd), off, len(buf))
	f.read = true
	return r, fs.OK
}

func (f *NFScache) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := syscall.Pwrite(f.fd, data, off)
	if err == nil {
		n64 := int64(n)
		if off+n64 > f.st.Size {
			f.st.Size = off + n64
			f.st.Blocks = ((4095 + f.st.Size) >> 12) << 3
		}
		f.write = true
	}
	return uint32(n), fs.ToErrno(err)
}

/*
var _ = (fs.FileGetattrer)((*NFScache)(nil))
var _ = (fs.FileReleaser)((*NFScache)(nil))

func (f *NFScache) Getattr(ctx context.Context, a *fuse.AttrOut) syscall.Errno {
	//TODO: together with metadata stat
	f.mu.Lock()
	defer f.mu.Unlock()
	err := syscall.Fstat(f.fd, &f.st)
	if err != nil {
		return fs.ToErrno(err)
	}
	a.FromStat(&f.st)

	return fs.OK
}

func (f *NFScache) Release(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fd != -1 {
		err := syscall.Close(f.fd)
		f.fd = -1
		return fs.ToErrno(err)
	}
	return syscall.EBADF
}*/

/*
var _ = (FileGetlker)((*nfsCache)(nil))
var _ = (FileSetlker)((*nfsCache)(nil))
var _ = (FileSetlkwer)((*nfsCache)(nil))
var _ = (FileLseeker)((*nfsCache)(nil))
var _ = (FileFlusher)((*nfsCache)(nil))
var _ = (FileFsyncer)((*nfsCache)(nil))
var _ = (FileSetattrer)((*nfsCache)(nil))
var _ = (FileAllocater)((*nfsCache)(nil))

func (f *nfsCache) Flush(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Since Flush() may be called for each dup'd fd, we don't
	// want to really close the file, we just want to flush. This
	// is achieved by closing a dup'd fd.
	newFd, err := syscall.Dup(f.fd)

	if err != nil {
		return ToErrno(err)
	}
	err = syscall.Close(newFd)
	return ToErrno(err)
}

func (f *nfsCache) Fsync(ctx context.Context, flags uint32) (errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := ToErrno(syscall.Fsync(f.fd))

	return r
}

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

func (f *nfsCache) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if errno := f.setAttr(ctx, in); errno != 0 {
		return errno
	}

	return f.Getattr(ctx, out)
}

func (f *nfsCache) setAttr(ctx context.Context, in *fuse.SetAttrIn) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	var errno syscall.Errno
	if mode, ok := in.GetMode(); ok {
		errno = ToErrno(syscall.Fchmod(f.fd, mode))
		if errno != 0 {
			return errno
		}
	}

	uid32, uOk := in.GetUID()
	gid32, gOk := in.GetGID()
	if uOk || gOk {
		uid := -1
		gid := -1

		if uOk {
			uid = int(uid32)
		}
		if gOk {
			gid = int(gid32)
		}
		errno = ToErrno(syscall.Fchown(f.fd, uid, gid))
		if errno != 0 {
			return errno
		}
	}

	mtime, mok := in.GetMTime()
	atime, aok := in.GetATime()

	if mok || aok {
		ap := &atime
		mp := &mtime
		if !aok {
			ap = nil
		}
		if !mok {
			mp = nil
		}
		errno = f.utimens(ap, mp)
		if errno != 0 {
			return errno
		}
	}

	if sz, ok := in.GetSize(); ok {
		errno = ToErrno(syscall.Ftruncate(f.fd, int64(sz)))
		if errno != 0 {
			return errno
		}
	}
	return OK
}

func (f *nfsCache) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := unix.Seek(f.fd, int64(off), int(whence))
	return uint64(n), ToErrno(err)
}
*/
