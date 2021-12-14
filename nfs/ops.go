package nfs

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/sys/unix"
)

var logger = logging.Logger("nfs")

// NFSRoot holds the parameters for creating a new network
// filesystem. Network filesystem splits meta and data layer.
type NFSRoot struct {
	// The path to the root of the underlying file system.
	Path string

	// The device on which the Path resides. This must be set if
	// the underlying filesystem crosses file systems.
	Dev uint64

	// The meta store on the local machine.
	*MetaStore

	// nextNodeID is the next free NodeID. Increment after copying the value.
	nextNodeId uint64

	// The in-memory shared data for open files indexed with inode number.
	openStats

	// NewNode returns a new InodeEmbedder to be used to respond
	// to a LOOKUP/CREATE/MKDIR/MKNOD opcode. If not set, use a
	// LoopbackNode.
	NewNode func(rootData *NFSRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder
}

func (r *NFSRoot) getOpenStat(self *fs.Inode) *openStat {
	return r.openStats.getOpenStat(self.StableAttr().Ino)
}

func (r *NFSRoot) applyOpenStat(self *fs.Inode, st *syscall.Stat_t) *openStat {
	return r.openStats.applyOpenStat(self.StableAttr().Ino, st)
}

func (r *NFSRoot) releaseOpenStat(self *fs.Inode) {
	if st, rm := r.openStats.releaseOpenStat(self.StableAttr().Ino); rm {
		r.delete(self)
	} else if st != nil {
		r.setattr(self, st)
	}
}

// snapshotOpenStat sync attr from openstat to db
func (r *NFSRoot) snapshotOpenStat(self *fs.Inode) {
	if st := r.openStats.snapshotOpenStat(self.StableAttr().Ino); st != nil {
		r.setattr(self, st)
	}
}

func (r *NFSRoot) insert(parent *fs.Inode, name string, st *syscall.Stat_t, gen uint64, target string) error {
	logger.Infof("ino %v, gen %v, pino %v, name %v", st.Ino, gen, parent.StableAttr().Ino, name)
	return r.MetaStore.Insert(parent.StableAttr().Ino, name, st, gen, target)
}

func (r *NFSRoot) setattr(self *fs.Inode, st *syscall.Stat_t) error {
	return r.MetaStore.Setattr(self.StableAttr().Ino, st)
}

func (r *NFSRoot) getattr(self *fs.Inode) *syscall.Stat_t {
	i := r.MetaStore.Lookup(self.StableAttr().Ino)
	return &i.Stat
}

// readdir lookups one of parent, self, children
func (r *NFSRoot) readdir(self *fs.Inode) []*Item {
	var i *Item
	_, pr := self.Parent()
	if pr != nil {
		i = r.MetaStore.Lookup(pr.StableAttr().Ino)
	} else {
		i = r.MetaStore.Lookup(RootBin)
	}
	i.Link.Name = ".."

	is := r.MetaStore.ReadDir(self.StableAttr().Ino)
	is[0].Link.Name = "."
	is = append(is, i)
	return is
}

func (r *NFSRoot) readlink(self *fs.Inode) string {
	i := r.MetaStore.Lookup(self.StableAttr().Ino)
	return i.Symlink
}

func (r *NFSRoot) lookup(parent *fs.Inode, name string) *Item {
	return r.MetaStore.LookupDentry(parent.StableAttr().Ino, name)
}

func (r *NFSRoot) delete(self *fs.Inode) error {
	return r.MetaStore.SoftDelete(self.StableAttr().Ino)
}

func (r *NFSRoot) applyIno() (uint64, uint64) {
	if ino, gen := r.MetaStore.ApplyIno(); ino > 0 {
		return ino, gen + 1
	} else {
		return atomic.AddUint64(&r.nextNodeId, 1) - 1, 1
	}
}

func (r *NFSRoot) isEmptyDir(self *fs.Inode) bool {
	return r.MetaStore.IsEmptyDir(self.StableAttr().Ino)
}

func (r *NFSRoot) replaceOpen(src, dst, dstDir *fs.Inode, dstname string, now *syscall.Timespec) error {
	return r.MetaStore.ReplaceOpen(src.StableAttr().Ino, dst.StableAttr().Ino, dstDir.StableAttr().Ino, dstname, now)
}

func (r *NFSRoot) replace(src, dst, dstDir *fs.Inode, dstname string, now *syscall.Timespec) error {
	return r.MetaStore.Replace(src.StableAttr().Ino, dst.StableAttr().Ino, dstDir.StableAttr().Ino, dstname, now)
}

func (r *NFSRoot) exchange(src, srcDir, dst, dstDir *fs.Inode, srcname, dstname string, now *syscall.Timespec) error {
	return r.MetaStore.Exchange(src.StableAttr().Ino, srcDir.StableAttr().Ino, dst.StableAttr().Ino, dstDir.StableAttr().Ino, srcname, dstname, now)
}

func (r *NFSRoot) rename(src, dstDir *fs.Inode, dstname string, now *syscall.Timespec) error {
	return r.MetaStore.Rename(src.StableAttr().Ino, dstDir.StableAttr().Ino, dstname, now)
}

func (r *NFSRoot) resolve(path string) *Item {
	return r.MetaStore.Resolve(path)
}

func (r *NFSRoot) newNode(parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
	return &NFSNode{
		RootData: r,
	}
}

func idFromStat(st *syscall.Stat_t, gen uint64) fs.StableAttr {
	return fs.StableAttr{
		Mode: uint32(st.Mode),
		Gen:  gen,
		Ino:  st.Ino,
	}
}

// NewNFSRoot returns a root node for a network file system whose
// root is at the given root. This node implements all NodeXxxxer
// operations available.
func NewNFSRoot(rootPath string, store *MetaStore) (*NFSRoot, fs.InodeEmbedder, error) {
	var st syscall.Stat_t
	err := syscall.Stat(rootPath, &st)
	if err != nil {
		return nil, nil, err
	}

	root := &NFSRoot{
		Path:      rootPath,
		Dev:       uint64(st.Dev),
		MetaStore: store,
		openStats: openStats{stats: map[uint64]*openStat{}},
	}

	root.nextNodeId = store.NextAllocateIno()

	logger.Infof("next ino %v", root.nextNodeId)
	if root.nextNodeId == 1 {
		var gen uint64
		st.Ino, gen = root.applyIno()
		if err := root.MetaStore.Insert(RootBin, "/", &st, gen, ""); err != nil {
			return nil, nil, err
		}
	}

	if err := root.MetaStore.CollectTempIno(); err != nil {
		return nil, nil, err
	}

	return root, root.newNode(nil, "", &st), nil
}

// NFSNode is a filesystem node in a loopback file system. It is
// public so it can be used as a basis for other loopback based
// filesystems. See NewLoopbackFile or LoopbackRoot for more
// information.
type NFSNode struct {
	fs.Inode

	// RootData points back to the root of the loopback filesystem.
	RootData *NFSRoot
}

// path returns the full path to the file in the underlying file
// system.
func (n *NFSNode) path() string {
	path := n.Path(n.Root())
	return filepath.Join(n.RootData.Path, path)
}

var _ = (fs.NodeGetattrer)((*NFSNode)(nil))
var _ = (fs.FileHandle)((*NFScache)(nil))

func (n *NFSNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logger.Infof("getattr %s", n.Path(n.Root()))

	// go-fuse searches any existing filehandle, then it is doable to translate getattr to fgetattr.
	self := n.EmbeddedInode()
	if o := n.RootData.getOpenStat(self); o != nil {
		o.mu.Lock()
		out.FromStat(&o.st)
		o.mu.Unlock()
	} else {
		st := n.RootData.getattr(self)

		if st.Ino == 0 {
			return fs.ToErrno(os.ErrNotExist)
		}
		out.FromStat(st)
	}
	return fs.OK
}

var _ = (fs.NodeReleaser)((*NFSNode)(nil))

func (n *NFSNode) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	self := n.EmbeddedInode()
	n.RootData.releaseOpenStat(self)
	c := f.(*NFScache)
	logger.Infof("release  %s, %v", n.Path(n.Root()), c.getAttr())
	return c.Close()
}

var _ = (fs.NodeLookuper)((*NFSNode)(nil))

func (n *NFSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	pr := n.EmbeddedInode()
	logger.Infof("lookup  %s/%s", n.Path(n.Root()), name)
	i := n.RootData.lookup(pr, name)
	if i.Ino == 0 {
		return nil, fs.ToErrno(os.ErrNotExist)
	}

	out.Attr.FromStat(&i.Stat)
	node := n.RootData.newNode(pr, name, &i.Stat)
	ch := n.NewInode(ctx, node, idFromStat(&i.Stat, i.Gen))
	return ch, 0
}

var _ = (fs.NodeCreater)((*NFSNode)(nil))

func (n *NFSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	logger.Infof("create %s/%s, %o", n.Path(n.Root()), name, mode)
	st := syscall.Stat_t{Mode: mode | syscall.S_IFREG, Blksize: syscall.S_BLKSIZE, Nlink: 1}
	n.preserveOwner(ctx, &st)

	pr := n.EmbeddedInode()
	ch, err := n.newChild(ctx, pr, name, &st, "")
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	flags = flags &^ syscall.O_APPEND
	fd, err := syscall.Open(n.cachePath(ch), int(flags)|os.O_CREATE, mode)
	if err != nil {
		n.RootData.delete(ch)
		return nil, nil, 0, fs.ToErrno(err)
	}

	os := n.RootData.applyOpenStat(ch, &st)

	lf := NewNFSCache(fd, os)
	out.FromStat(&st)
	return ch, lf, 0, 0
}

func (n *NFSNode) newChild(ctx context.Context, parent *fs.Inode, name string, st *syscall.Stat_t, target string) (*fs.Inode, error) {
	var gen uint64
	st.Ino, gen = n.RootData.applyIno()
	err := n.RootData.insert(parent, name, st, gen, target)
	if err != nil {
		return nil, err
	}

	node := n.RootData.newNode(parent, name, st)
	ch := n.NewInode(ctx, node, idFromStat(st, gen))

	return ch, nil
}

// preserveOwner sets uid and gid of `path` according to the caller information
// in `ctx`.
func (n *NFSNode) preserveOwner(ctx context.Context, st *syscall.Stat_t) {
	/*if os.Getuid() != 0 {
		return nil
	}*/
	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return
	}

	st.Uid = caller.Uid
	st.Gid = caller.Gid
	t := nowTimespec()
	st.Atim, st.Mtim, st.Ctim = t, t, t
}

var _ = (fs.NodeMkdirer)((*NFSNode)(nil))

func (n *NFSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logger.Infof("mkdir %s/%s, %o", n.Path(n.Root()), name, mode)
	st := syscall.Stat_t{Mode: mode | syscall.S_IFDIR, Blksize: syscall.S_BLKSIZE, Nlink: 1}
	n.preserveOwner(ctx, &st)
	pr := n.EmbeddedInode()
	ch, err := n.newChild(ctx, pr, name, &st, "")
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)

	return ch, 0
}

var _ = (fs.NodeUnlinker)((*NFSNode)(nil))
var _ = (fs.NodeRmdirer)((*NFSNode)(nil))

func (n *NFSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	pr := n.EmbeddedInode()
	ch := pr.GetChild(name)
	if ch == nil {
		return fs.OK
	}

	logger.Infof("Rmdir %s/%s, %v", n.Path(n.Root()), name, ch.StableAttr().Ino)

	if !n.RootData.isEmptyDir(ch) {
		return syscall.ENOTEMPTY
	}
	n.RootData.delete(ch)
	return fs.OK
}

func (n *NFSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	//TODO: hardlink feature, skip it for now
	pr := n.EmbeddedInode()
	ch := pr.GetChild(name)
	if ch == nil {
		return fs.OK
	}

	logger.Infof("Unlink /%s/%s", n.Path(n.Root()), name)
	//TODO: ignore unlink cache error?
	err := syscall.Unlink(n.cachePath(ch))

	// Update Nlink of openstat to 0
	if os := n.RootData.getOpenStat(ch); os != nil {
		os.mu.Lock()
		os.st.Nlink -= 1
		if os.st.Nlink == 0 {
			os.deferDel = true
		}
		os.mu.Unlock()
	} else {
		n.RootData.delete(ch)
	}
	return fs.ToErrno(err)
}

var _ = (fs.NodeRenamer)((*NFSNode)(nil))

func (n *NFSNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags&unix.RENAME_WHITEOUT == unix.RENAME_WHITEOUT {
		return syscall.EINVAL
	}

	if flags&(unix.RENAME_NOREPLACE|unix.RENAME_EXCHANGE) == (unix.RENAME_NOREPLACE | unix.RENAME_EXCHANGE) {
		return syscall.EINVAL
	}

	pr1 := n.EmbeddedInode()
	ch1 := pr1.GetChild(name)
	pr2 := newParent.EmbeddedInode()
	ch2 := pr2.GetChild(newName)
	op := RENAME
	now := nowTimespec()

	if flags&unix.RENAME_NOREPLACE == unix.RENAME_NOREPLACE {
		if ch2 != nil {
			return syscall.EEXIST
		}
	} else if flags&unix.RENAME_EXCHANGE == unix.RENAME_EXCHANGE {
		if ch2 == nil {
			return syscall.ENOENT
		}
		op = EXCHANGE
		if os := n.RootData.getOpenStat(ch2); os != nil {
			os.mu.Lock()
			os.st.Ctim = now
			os.mu.Unlock()
		}
	} else {
		if ch2 != nil {
			// if target is dir, check it is empty
			if ch2.StableAttr().Mode&syscall.S_IFDIR != 0 && !n.RootData.isEmptyDir(ch2) {
				return syscall.ENOTEMPTY
				// if target is file, delete cache
			} else if ch2.StableAttr().Mode&syscall.S_IFREG != 0 {
				syscall.Unlink(n.cachePath(ch2))
			}

			// Minus1 Nlink of openstat
			op = REPLACE
			if os := n.RootData.getOpenStat(ch2); os != nil {
				os.mu.Lock()
				os.st.Nlink -= 1
				if os.st.Nlink == 0 {
					os.deferDel = true
					op = REPLACE_OPEN
				}
				os.mu.Unlock()
			}
		}
	}

	if os := n.RootData.getOpenStat(ch1); os != nil {
		os.mu.Lock()
		os.st.Ctim = now
		os.mu.Unlock()
	}

	var err error
	switch op {
	case RENAME:
		err = n.RootData.rename(ch1, pr2, newName, &now)
	case REPLACE:
		err = n.RootData.replace(ch1, ch2, pr2, newName, &now)
	case REPLACE_OPEN:
		err = n.RootData.replaceOpen(ch1, ch2, pr2, newName, &now)
	case EXCHANGE:
		err = n.RootData.exchange(ch1, pr1, ch2, pr2, name, newName, &now)
	}

	return fs.ToErrno(err)
}

func (n *NFSNode) cachePath(self *fs.Inode) string {
	//TODO: split caches, prevent large_dir perf regression
	return filepath.Join(n.RootData.Path, strconv.FormatUint(self.StableAttr().Ino, 10))
}

var _ = (fs.NodeOpener)((*NFSNode)(nil))

func (n *NFSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	self := n.EmbeddedInode()
	flags = flags &^ syscall.O_APPEND
	p := n.cachePath(self)
	f, err := syscall.Open(p, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	st := n.RootData.getattr(self)
	os := n.RootData.applyOpenStat(self, st)
	lf := NewNFSCache(f, os)
	return lf, 0, 0
}

var _ = (fs.NodeFlusher)((*NFSNode)(nil))

func (n *NFSNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	self := n.EmbeddedInode()
	c := f.(*NFScache)

	// Since Flush() may be called for each dup'd fd, we don't
	// want to really close the file, we just want to flush. This
	// is achieved by closing a dup'd fd.
	if newFd, err := syscall.Dup(c.fd); err != nil {
		return fs.ToErrno(err)
	} else if err := syscall.Close(newFd); err != nil {
		return fs.ToErrno(err)
	}

	n.RootData.snapshotOpenStat(self)
	return fs.OK
}

var _ = (fs.NodeFsyncer)((*NFSNode)(nil))

func (n *NFSNode) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	self := n.EmbeddedInode()
	c := f.(*NFScache)

	if err := syscall.Fsync(c.fd); err != nil {
		return fs.ToErrno(err)
	}

	n.RootData.snapshotOpenStat(self)

	return fs.OK
}

var _ = (fs.NodeSetattrer)((*NFSNode)(nil))

func (n *NFSNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) (errno syscall.Errno) {
	var c *NFScache
	self := n.EmbeddedInode()
	if f == nil {
		p := n.cachePath(n.EmbeddedInode())
		fd, err := syscall.Open(p, syscall.O_RDWR, 0)
		if err != nil {
			return fs.ToErrno(err)
		}
		st := n.RootData.getattr(self)
		os := n.RootData.applyOpenStat(self, st)
		defer n.RootData.releaseOpenStat(self)
		c = NewNFSCache(fd, os).(*NFScache)
	} else {
		c = f.(*NFScache)
	}

	if errno := c.setAttr(in); errno != 0 {
		return errno
	}

	out.FromStat(c.getAttr())

	return fs.OK
}

var _ = (fs.NodeReadlinker)((*NFSNode)(nil))

func (n *NFSNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	self := n.EmbeddedInode()
	target := n.RootData.readlink(self)
	return []byte(target), 0
}

var _ = (fs.NodeSymlinker)((*NFSNode)(nil))

func (n *NFSNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	pr := n.EmbeddedInode()
	st := syscall.Stat_t{Mode: 0755 | syscall.S_IFLNK, Size: int64(len(target)), Blksize: syscall.S_BLKSIZE, Nlink: 1}
	n.preserveOwner(ctx, &st)

	ch, err := n.newChild(ctx, pr, name, &st, target)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)
	return ch, 0
}

var _ = (fs.NodeOpendirer)((*NFSNode)(nil))

func (n *NFSNode) Opendir(ctx context.Context) syscall.Errno {
	//TODO: May use this to trigger sync dir content
	//TODO: share dirstream
	return fs.OK
}

var _ = (fs.NodeReaddirer)((*NFSNode)(nil))

func (n *NFSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return NewNFSDirStream(n.EmbeddedInode(), n.RootData.readdir)
}

var _ = (fs.NodeMknoder)((*NFSNode)(nil))

func (n *NFSNode) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	/*
	 * Open O_CREAT | O_EXCL over NFSv4 to FUSE will trigger this behavior.
	 * Just give owner permission to read/write.
	 * https://github.com/trapexit/mergerfs/issues/343
	 */
	if mode&0777 == 0 {
		mode |= 0700
	}

	st := syscall.Stat_t{Mode: mode, Rdev: uint64(rdev), Blksize: syscall.S_BLKSIZE, Nlink: 1}
	n.preserveOwner(ctx, &st)
	pr := n.EmbeddedInode()
	ch, err := n.newChild(ctx, pr, name, &st, "")
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	err = syscall.Mknod(n.cachePath(ch), mode, int(rdev))
	if err != nil {
		n.RootData.delete(ch)
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)

	return ch, 0
}

/*var _ = (NodeStatfser)((*NFSNode)(nil))
var _ = (NodeGetattrer)((*NFSNode)(nil))
var _ = (NodeGetxattrer)((*NFSNode)(nil))
var _ = (NodeSetxattrer)((*NFSNode)(nil))
var _ = (NodeRemovexattrer)((*NFSNode)(nil))
var _ = (NodeListxattrer)((*NFSNode)(nil))
var _ = (NodeCopyFileRanger)((*NFSNode)(nil))
var _ = (NodeLinker)((*NFSNode)(nil))

func (n *NFSNode) Statfs(ctx context.Context, out *fs.StatfsOut) syscall.Errno {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(n.path(), &s)
	if err != nil {
		return fs.ToErrno(err)
	}
	out.FromStatfsT(&s)
	return fs.OK
}

func (n *NFSNode) Link(ctx context.Context, target InodeEmbedder, name string, out *fs.EntryOut) (*Inode, syscall.Errno) {

	p := filepath.Join(n.path(), name)
	err := syscall.Link(filepath.Join(n.RootData.Path, target.EmbeddedInode().Path(nil)), p)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Unlink(p)
		return nil, fs.ToErrno(err)
	}
	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, idFromStat(&st))

	out.Attr.FromStat(&st)
	return ch, 0
}
*/
