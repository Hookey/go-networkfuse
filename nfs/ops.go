// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package nfs

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("nfs")

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

func (r *NFSRoot) insert(parent *fs.Inode, name string, st *syscall.Stat_t) error {
	//i := &Item{Ino: st.Ino, PIno: parent.StableAttr().Ino, Name: name, Stat: st}
	return r.MetaStore.Insert(parent.StableAttr().Ino, name, st)
}

func (r *NFSRoot) getattr(self *fs.Inode, st *syscall.Stat_t) (err error) {
	*st, err = r.MetaStore.Getattr(self.StableAttr().Ino)
	return
}

func (r *NFSRoot) lookup(parent *fs.Inode, name string, st *syscall.Stat_t) (err error) {
	*st, err = r.MetaStore.Lookup(parent.StableAttr().Ino, name)
	return
}

func (r *NFSRoot) delete(self *fs.Inode) (err error) {
	//err = r.MetaStore.Delete(self.StableAttr().Ino)
	err = r.MetaStore.SoftDelete(self.StableAttr().Ino)
	return
}

func (r *NFSRoot) deleteDentry(parent *fs.Inode, name string) (err error) {
	err = r.MetaStore.DeleteDentry(parent.StableAttr().Ino, name)
	return
}

func (r *NFSRoot) applyIno() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.nextNodeId
	r.nextNodeId++

	return a
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
		Path:       rootPath,
		Dev:        uint64(st.Dev),
		MetaStore:  store,
		nextNodeId: 2, //TODO: find next empty when mounting
	}

	st.Ino = 1
	//TODO: check root exists, or insert root to DB
	if err := root.MetaStore.Insert(0, "/", &st); err != nil {
		return nil, err
	}

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

func (n *NFSNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	//TODO: fh getattr
	/*if f != nil {
		return f.(FileGetattrer).Getattr(ctx, out)
	}*/

	log.Infof("getattr %s", n.Path(n.Root()))

	st := syscall.Stat_t{}
	self := n.EmbeddedInode()
	err := n.RootData.getattr(self, &st)
	log.Info(err)

	if err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	return fs.OK
}

var _ = (fs.NodeLookuper)((*NFSNode)(nil))

func (n *NFSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	st := syscall.Stat_t{}
	pr := n.EmbeddedInode()
	log.Infof("lookup  %s %s", n.Path(n.Root()), name)
	err := n.RootData.lookup(pr, name, &st)
	log.Info(st.Ino, err)
	if st.Ino == 0 {
		return nil, fs.ToErrno(os.ErrNotExist)
	}

	out.Attr.FromStat(&st)
	node := n.RootData.newNode(pr, name, &st)
	ch := n.NewInode(ctx, node, idFromStat(&st, 1))
	return ch, 0
}

var _ = (fs.NodeCreater)((*NFSNode)(nil))

func (n *NFSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Infof("create %s, %s, %o", n.Path(n.Root()), name, mode)
	st := syscall.Stat_t{Ino: n.RootData.applyIno(), Mode: mode | syscall.S_IFREG, Blksize: syscall.S_BLKSIZE}

	pr := n.EmbeddedInode()
	err := n.RootData.insert(pr, name, &st)
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	flags = flags &^ syscall.O_APPEND
	cachePath := filepath.Join(n.RootData.Path, strconv.FormatUint(st.Ino, 10))
	fd, err := syscall.Open(cachePath, int(flags)|os.O_CREATE, mode)
	if err != nil {
		//TODO: delete meta
		return nil, nil, 0, fs.ToErrno(err)
	}

	node := n.RootData.newNode(pr, name, &st)
	ch := n.NewInode(ctx, node, idFromStat(&st, 1))
	lf := fs.NewLoopbackFile(fd)
	out.FromStat(&st)
	return ch, lf, 0, 0
}

// preserveOwner sets uid and gid of `path` according to the caller information
// in `ctx`.
/*func (n *NFSNode) preserveOwner(ctx context.Context, path string) error {
	if os.Getuid() != 0 {
		return nil
	}
	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return nil
	}
	return syscall.Lchown(path, int(caller.Uid), int(caller.Gid))
}*/

var _ = (fs.NodeMkdirer)((*NFSNode)(nil))

func (n *NFSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Infof("mkdir /%s/%s, %o", n.Path(n.Root()), name, mode)
	st := syscall.Stat_t{Ino: n.RootData.applyIno(), Mode: mode | syscall.S_IFDIR, Blksize: syscall.S_BLKSIZE}
	pr := n.EmbeddedInode()
	err := n.RootData.insert(pr, name, &st)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	node := n.RootData.newNode(pr, name, &st)
	ch := n.NewInode(ctx, node, idFromStat(&st, 1))
	out.Attr.FromStat(&st)

	return ch, 0
}

var _ = (fs.NodeUnlinker)((*NFSNode)(nil))
var _ = (fs.NodeRmdirer)((*NFSNode)(nil))

func (n *NFSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	//st := syscall.Stat_t{}
	pr := n.EmbeddedInode()
	ch := pr.GetChild(name)
	if ch == nil {
		return fs.OK
	}

	log.Infof("Rmdir /%s/%s, %v", n.Path(n.Root()), name, ch.StableAttr().Ino)
	/*n.RootData.lookup(pr, name, &st)
	if st.Ino == 0 {
		return fs.OK
	}*/

	//TODO: check not empty dir
	//n.RootData.delete(pr, name)
	n.RootData.delete(ch)
	return fs.OK
}

func (n *NFSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	//st := syscall.Stat_t{}
	pr := n.EmbeddedInode()

	//TODO: hardlink feature, skip it for now
	ch := pr.GetChild(name)
	if ch == nil {
		return fs.OK
	}

	/*n.RootData.lookup(pr, name, &st)
	if st.Ino == 0 {
		return fs.OK
	}*/

	cachePath := filepath.Join(n.RootData.Path, strconv.FormatUint(ch.StableAttr().Ino, 10))
	log.Infof("Unlink /%s/%s, at %s", n.Path(n.Root()), name, cachePath)
	err := syscall.Unlink(cachePath)
	n.RootData.delete(ch)
	return fs.ToErrno(err)
}

/*var _ = (NodeStatfser)((*NFSNode)(nil))
var _ = (NodeStatfser)((*NFSNode)(nil))
var _ = (NodeGetattrer)((*NFSNode)(nil))
var _ = (NodeGetxattrer)((*NFSNode)(nil))
var _ = (NodeSetxattrer)((*NFSNode)(nil))
var _ = (NodeRemovexattrer)((*NFSNode)(nil))
var _ = (NodeListxattrer)((*NFSNode)(nil))
var _ = (NodeReadlinker)((*NFSNode)(nil))
var _ = (NodeOpener)((*NFSNode)(nil))
var _ = (NodeCopyFileRanger)((*NFSNode)(nil))
var _ = (NodeLookuper)((*NFSNode)(nil))
var _ = (NodeOpendirer)((*NFSNode)(nil))
var _ = (NodeReaddirer)((*NFSNode)(nil))
var _ = (NodeMkdirer)((*NFSNode)(nil))
var _ = (NodeMknoder)((*NFSNode)(nil))
var _ = (NodeLinker)((*NFSNode)(nil))
var _ = (NodeSymlinker)((*NFSNode)(nil))
var _ = (NodeUnlinker)((*NFSNode)(nil))
var _ = (NodeRmdirer)((*NFSNode)(nil))
var _ = (NodeRenamer)((*NFSNode)(nil))

func (n *NFSNode) Statfs(ctx context.Context, out *fs.StatfsOut) syscall.Errno {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(n.path(), &s)
	if err != nil {
		return fs.ToErrno(err)
	}
	out.FromStatfsT(&s)
	return fs.OK
}

func (n *NFSNode) Lookup(ctx context.Context, name string, out *fs.EntryOut) (*Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)

	st := syscall.Stat_t{}
	err := syscall.Lstat(p, &st)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)
	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, idFromStat(&st))
	return ch, 0
}

// preserveOwner sets uid and gid of `path` according to the caller information
// in `ctx`.
func (n *NFSNode) preserveOwner(ctx context.Context, path string) error {
	if os.Getuid() != 0 {
		return nil
	}
	caller, ok := fs.FromContext(ctx)
	if !ok {
		return nil
	}
	return syscall.Lchown(path, int(caller.Uid), int(caller.Gid))
}

func (n *NFSNode) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fs.EntryOut) (*Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := syscall.Mknod(p, mode, int(rdev))
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Rmdir(p)
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)

	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, idFromStat(&st))

	return ch, 0
}

func (n *NFSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fs.EntryOut) (*Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := os.Mkdir(p, os.FileMode(mode))
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Rmdir(p)
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)

	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, idFromStat(&st))

	return ch, 0
}

func (n *NFSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	p := filepath.Join(n.path(), name)
	err := syscall.Rmdir(p)
	return fs.ToErrno(err)
}

func (n *NFSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	p := filepath.Join(n.path(), name)
	err := syscall.Unlink(p)
	return fs.ToErrno(err)
}

func (n *NFSNode) Rename(ctx context.Context, name string, newParent InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags&RENAME_EXCHANGE != 0 {
		return n.renameExchange(name, newParent, newName)
	}

	p1 := filepath.Join(n.path(), name)
	p2 := filepath.Join(n.RootData.Path, newParent.EmbeddedInode().Path(nil), newName)

	err := syscall.Rename(p1, p2)
	return fs.ToErrno(err)
}

var _ = (NodeCreater)((*NFSNode)(nil))

func (n *NFSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fs.EntryOut) (inode *Inode, fh FileHandle, fuseFlags uint32, errno syscall.Errno) {
	p := filepath.Join(n.path(), name)
	flags = flags &^ syscall.O_APPEND
	fd, err := syscall.Open(p, int(flags)|os.O_CREATE, mode)
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Fstat(fd, &st); err != nil {
		syscall.Close(fd)
		return nil, nil, 0, fs.ToErrno(err)
	}

	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, idFromStat(&st))
	lf := NewLoopbackFile(fd)

	out.FromStat(&st)
	return ch, lf, 0, 0
}

func (n *NFSNode) Symlink(ctx context.Context, target, name string, out *fs.EntryOut) (*Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := syscall.Symlink(target, p)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	n.preserveOwner(ctx, p)
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

func (n *NFSNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	p := n.path()

	for l := 256; ; l *= 2 {
		buf := make([]byte, l)
		sz, err := syscall.Readlink(p, buf)
		if err != nil {
			return nil, fs.ToErrno(err)
		}

		if sz < len(buf) {
			return buf[:sz], 0
		}
	}
}

func (n *NFSNode) Open(ctx context.Context, flags uint32) (fh FileHandle, fuseFlags uint32, errno syscall.Errno) {
	flags = flags &^ syscall.O_APPEND
	p := n.path()
	f, err := syscall.Open(p, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	lf := NewLoopbackFile(f)
	return lf, 0, 0
}

func (n *NFSNode) Opendir(ctx context.Context) syscall.Errno {
	fd, err := syscall.Open(n.path(), syscall.O_DIRECTORY, 0755)
	if err != nil {
		return fs.ToErrno(err)
	}
	syscall.Close(fd)
	return fs.OK
}

func (n *NFSNode) Readdir(ctx context.Context) (DirStream, syscall.Errno) {
	return NewLoopbackDirStream(n.path())
}

func (n *NFSNode) Getattr(ctx context.Context, f FileHandle, out *fs.AttrOut) syscall.Errno {
	if f != nil {
		return f.(FileGetattrer).Getattr(ctx, out)
	}

	p := n.path()

	var err error
	st := syscall.Stat_t{}
	if &n.Inode == n.Root() {
		err = syscall.Stat(p, &st)
	} else {
		err = syscall.Lstat(p, &st)
	}

	if err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	return fs.OK
}

var _ = (NodeSetattrer)((*NFSNode)(nil))

func (n *NFSNode) Setattr(ctx context.Context, f FileHandle, in *fs.SetAttrIn, out *fs.AttrOut) syscall.Errno {
	p := n.path()
	fsa, ok := f.(FileSetattrer)
	if ok && fsa != nil {
		fsa.Setattr(ctx, in, out)
	} else {
		if m, ok := in.GetMode(); ok {
			if err := syscall.Chmod(p, m); err != nil {
				return fs.ToErrno(err)
			}
		}

		uid, uok := in.GetUID()
		gid, gok := in.GetGID()
		if uok || gok {
			suid := -1
			sgid := -1
			if uok {
				suid = int(uid)
			}
			if gok {
				sgid = int(gid)
			}
			if err := syscall.Chown(p, suid, sgid); err != nil {
				return fs.ToErrno(err)
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
			var ts [2]syscall.Timespec
			ts[0] = fs.UtimeToTimespec(ap)
			ts[1] = fs.UtimeToTimespec(mp)

			if err := syscall.UtimesNano(p, ts[:]); err != nil {
				return fs.ToErrno(err)
			}
		}

		if sz, ok := in.GetSize(); ok {
			if err := syscall.Truncate(p, int64(sz)); err != nil {
				return fs.ToErrno(err)
			}
		}
	}

	fga, ok := f.(FileGetattrer)
	if ok && fga != nil {
		fga.Getattr(ctx, out)
	} else {
		st := syscall.Stat_t{}
		err := syscall.Lstat(p, &st)
		if err != nil {
			return fs.ToErrno(err)
		}
		out.FromStat(&st)
	}
	return fs.OK
}*/
