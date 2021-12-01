package nfs

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	logging "github.com/ipfs/go-log/v2"
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
	mu         sync.Mutex
	nextNodeId uint64

	// NewNode returns a new InodeEmbedder to be used to respond
	// to a LOOKUP/CREATE/MKDIR/MKNOD opcode. If not set, use a
	// LoopbackNode.
	NewNode func(rootData *NFSRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder
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

func (r *NFSRoot) replace(src, dst, dstDir *fs.Inode, dstname string) error {
	return r.MetaStore.Replace(src.StableAttr().Ino, dst.StableAttr().Ino, dstDir.StableAttr().Ino, dstname)
}

func (r *NFSRoot) rename(src, dstDir *fs.Inode, dstname string) error {
	return r.MetaStore.Rename(src.StableAttr().Ino, dstDir.StableAttr().Ino, dstname)
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
func NewNFSRoot(rootPath string, store *MetaStore) (fs.InodeEmbedder, error) {
	var st syscall.Stat_t
	err := syscall.Stat(rootPath, &st)
	if err != nil {
		return nil, err
	}

	root := &NFSRoot{
		Path:      rootPath,
		Dev:       uint64(st.Dev),
		MetaStore: store,
	}

	root.nextNodeId = store.NextAllocateIno()

	logger.Infof("next ino %v", root.nextNodeId)
	if root.nextNodeId == 1 {
		var gen uint64
		st.Ino, gen = root.applyIno()
		if err := root.MetaStore.Insert(RootBin, "/", &st, gen, ""); err != nil {
			return nil, err
		}
	}
	//TODO: abnormal shutdown handling
	//Move ino in temp bin to recycle bin

	return root.newNode(nil, "", &st), nil
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
	if f != nil {
		c := f.(*NFScache)
		c.mu.Lock()
		defer c.mu.Unlock()
		out.FromStat(&c.st)

		return fs.OK
		//return f.(fs.FileGetattrer).Getattr(ctx, out)
	}

	self := n.EmbeddedInode()
	st := n.RootData.getattr(self)

	if st.Ino == 0 {
		return fs.ToErrno(os.ErrNotExist)
	}
	out.FromStat(st)
	return fs.OK
}

var _ = (fs.NodeReleaser)((*NFSNode)(nil))

func (n *NFSNode) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	self := n.EmbeddedInode()
	c := f.(*NFScache)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fd != -1 {
		//TODO cache time
		logger.Infof("release  %s, %v", n.Path(n.Root()), c.st)
		c.UpdateTime()

		n.RootData.setattr(self, &c.st)
		err := syscall.Close(c.fd)
		c.fd = -1
		return fs.ToErrno(err)
	}
	return syscall.EBADF
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

	lf := NewNFSCache(fd, &st)
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
	// TODO: ignore unlink cache error?
	err := syscall.Unlink(n.cachePath(ch))
	n.RootData.delete(ch)
	return fs.ToErrno(err)
}

var _ = (fs.NodeRenamer)((*NFSNode)(nil))

func (n *NFSNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	//TODO: flags&RENAME_EXCHANGE
	//TODO: flags&RENAME_NOREPLACE
	pr1 := n.EmbeddedInode()
	ch1 := pr1.GetChild(name)
	pr2 := newParent.EmbeddedInode()
	ch2 := pr2.GetChild(newName)

	if ch2 != nil {
		// if target is dir, check it is empty
		if ch2.StableAttr().Mode&syscall.S_IFDIR != 0 && !n.RootData.isEmptyDir(ch2) {
			return syscall.ENOTEMPTY
			// if target is file, delete cache
		} else if ch2.StableAttr().Mode&syscall.S_IFREG != 0 {
			syscall.Unlink(n.cachePath(ch2))
		}

		//TODO update stat, ctime...
		err := n.RootData.replace(ch1, ch2, pr2, newName)
		return fs.ToErrno(err)
	} else {
		err := n.RootData.rename(ch1, pr2, newName)
		return fs.ToErrno(err)
	}
}

func (n *NFSNode) cachePath(self *fs.Inode) string {
	//TODO: split caches, prevent large_dir perf regression
	return filepath.Join(n.RootData.Path, strconv.FormatUint(self.StableAttr().Ino, 10))
}

var _ = (fs.NodeOpener)((*NFSNode)(nil))

func (n *NFSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	flags = flags &^ syscall.O_APPEND
	p := n.cachePath(n.EmbeddedInode())
	f, err := syscall.Open(p, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	st := n.RootData.getattr(n.EmbeddedInode())
	lf := NewNFSCache(f, st)
	return lf, 0, 0
}

var _ = (fs.NodeFlusher)((*NFSNode)(nil))

func (n *NFSNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	self := n.EmbeddedInode()
	c := f.(*NFScache)
	c.mu.Lock()
	defer c.mu.Unlock()

	c.UpdateTime()
	// Since Flush() may be called for each dup'd fd, we don't
	// want to really close the file, we just want to flush. This
	// is achieved by closing a dup'd fd.
	if newFd, err := syscall.Dup(c.fd); err != nil {
		return fs.ToErrno(err)
	} else if err := syscall.Close(newFd); err != nil {
		return fs.ToErrno(err)
	}

	n.RootData.setattr(self, &c.st)
	return fs.OK
}

var _ = (fs.NodeFsyncer)((*NFSNode)(nil))

func (n *NFSNode) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	self := n.EmbeddedInode()
	c := f.(*NFScache)
	c.mu.Lock()
	defer c.mu.Unlock()

	c.UpdateTime()
	err := syscall.Fsync(c.fd)
	if err == nil {
		n.RootData.setattr(self, &c.st)
	}

	return fs.ToErrno(err)
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
		c = NewNFSCache(fd, st).(*NFScache)
	} else {
		c = f.(*NFScache)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	mode, ok := in.GetMode()
	if ok {
		c.st.Mode = mode
	}

	uid32, uok := in.GetUID()
	if uok {
		c.st.Uid = uid32
	}

	gid32, gok := in.GetGID()
	if gok {
		c.st.Gid = gid32
	}

	mtime, mok := in.GetMTime()
	if mok {
		c.st.Mtim.Sec = mtime.Unix()
		c.st.Mtim.Nsec = int64(mtime.Nanosecond())
	}

	atime, aok := in.GetATime()
	if aok {
		c.st.Atim.Sec = atime.Unix()
		c.st.Atim.Nsec = int64(atime.Nanosecond())
	}

	sz, sok := in.GetSize()
	if sok {
		c.st.Size = int64(sz)
		c.st.Blocks = ((4095 + c.st.Size) >> 12) << 3
		errno = fs.ToErrno(syscall.Ftruncate(c.fd, int64(sz)))
		if errno != 0 {
			return errno
		}
	}

	if f == nil {
		n.RootData.setattr(self, &c.st)
	}
	out.FromStat(&c.st)

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
