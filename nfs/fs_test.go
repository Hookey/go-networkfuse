package nfs

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/posixtest"
	"golang.org/x/sys/unix"
)

type testCase struct {
	*testing.T

	dir     string
	origDir string
	mntDir  string
	dbDir   string

	nfs    fs.InodeEmbedder
	rawFS  fuse.RawFileSystem
	server *fuse.Server
	store  *MetaStore
}

func (tc *testCase) Clean() {
	if err := tc.server.Unmount(); err != nil {
		tc.Fatal(err)
	}
	tc.store.Close()
	if err := os.RemoveAll(tc.dir); err != nil {
		tc.Fatal(err)
	}
}

type testOptions struct {
	entryCache    bool
	attrCache     bool
	suppressDebug bool
	testDir       string
	ro            bool
}

// newTestCase creates the directories `orig` and `mnt` inside a temporary
// directory and mounts a loopback filesystem, backed by `orig`, on `mnt`.
func newTestCase(t *testing.T, opts *testOptions) *testCase {
	if opts == nil {
		opts = &testOptions{}
	}
	if opts.testDir == "" {
		opts.testDir, _ = ioutil.TempDir("", "")
	}
	tc := &testCase{
		dir: opts.testDir,
		T:   t,
	}
	tc.origDir = tc.dir + "/orig"
	tc.mntDir = tc.dir + "/mnt"
	tc.dbDir = tc.dir + "/db"
	if err := os.Mkdir(tc.origDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(tc.mntDir, 0755); err != nil {
		t.Fatal(err)
	}

	var err error
	tc.store, err = NewMetaStore(tc.dbDir)
	if err != nil {
		t.Fatalf("NewDB(%s): %v\n", tc.dbDir, err)
	}

	_, tc.nfs, err = NewNFSRoot(tc.origDir, tc.store, "")
	if err != nil {
		t.Fatalf("NewNFS: %v", err)
	}

	oneSec := time.Second

	attrDT := &oneSec
	if !opts.attrCache {
		attrDT = nil
	}
	entryDT := &oneSec
	if !opts.entryCache {
		entryDT = nil
	}
	tc.rawFS = fs.NewNodeFS(tc.nfs, &fs.Options{
		EntryTimeout: entryDT,
		AttrTimeout:  attrDT,
		Logger:       log.New(os.Stderr, "", 0),
	})

	mOpts := &fuse.MountOptions{}
	if !opts.suppressDebug {
		mOpts.Debug = verboseTest()
	}
	if opts.ro {
		mOpts.Options = append(mOpts.Options, "ro")
	}
	tc.server, err = fuse.NewServer(tc.rawFS, tc.mntDir, mOpts)
	if err != nil {
		t.Fatal(err)
	}

	go tc.server.Serve()
	if err := tc.server.WaitMount(); err != nil {
		t.Fatal(err)
	}
	return tc
}

// verboseTest returns true if the testing framework is run with -v.
func verboseTest() bool {
	var buf [2048]byte
	n := runtime.Stack(buf[:], false)
	if bytes.Index(buf[:n], []byte("TestNonVerbose")) != -1 {
		return false
	}

	flag := flag.Lookup("test.v")
	return flag != nil && flag.Value.String() == "true"
}

func TestBasic(t *testing.T) {
	tc := newTestCase(t, &testOptions{attrCache: true, entryCache: true})
	defer tc.Clean()

	fn := tc.mntDir + "/file"
	if err := ioutil.WriteFile(fn, []byte("hello"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	fi, err := os.Lstat(fn)
	if err != nil {
		t.Fatalf("Lstat: %v", err)
	}

	if fi.Size() != 5 {
		t.Errorf("got size %d want 5", fi.Size())
	}

	stat := fuse.ToStatT(fi)
	if got, want := uint32(stat.Mode), uint32(fuse.S_IFREG|0644); got != want {
		t.Errorf("got mode %o, want %o", got, want)
	}

	if err := os.Remove(fn); err != nil {
		t.Errorf("Remove: %v", err)
	}

	if fi, err := os.Lstat(fn); err == nil {
		t.Errorf("Lstat after remove: got file %v", fi)
	}
}

func TestGetAttrParallel(t *testing.T) {
	// We grab a file-handle to provide to the API so rename+fstat
	// can be handled correctly. Here, test that closing and
	// (f)stat in parallel don't lead to fstat on closed files.
	// We can only test that if we switch off caching
	tc := newTestCase(t, &testOptions{suppressDebug: true})
	defer tc.Clean()

	N := 100

	var fds []int
	var fns []string
	for i := 0; i < N; i++ {
		fn := filepath.Join(tc.mntDir, fmt.Sprintf("file%d", i))
		if err := ioutil.WriteFile(fn, []byte("hello"), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		fns = append(fns, fn)
		fd, err := syscall.Open(fn, syscall.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}

		fds = append(fds, fd)
	}

	var wg sync.WaitGroup
	wg.Add(2 * N)
	for i := 0; i < N; i++ {
		go func(i int) {
			if err := syscall.Close(fds[i]); err != nil {
				t.Errorf("close %d: %v", i, err)
			}
			wg.Done()
		}(i)
		go func(i int) {
			var st syscall.Stat_t
			if err := syscall.Lstat(fns[i], &st); err != nil {
				t.Errorf("lstat %d: %v", i, err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestReadDirStress(t *testing.T) {
	tc := newTestCase(t, &testOptions{suppressDebug: true, attrCache: true, entryCache: true})
	defer tc.Clean()

	// Create 110 entries
	for i := 0; i < 110; i++ {
		name := fmt.Sprintf("file%036x", i)
		if err := ioutil.WriteFile(filepath.Join(tc.mntDir, name), []byte("hello"), 0644); err != nil {
			t.Fatalf("WriteFile %q: %v", name, err)
		}
	}

	var wg sync.WaitGroup
	stress := func(gr int) {
		defer wg.Done()
		for i := 1; i < 10; i++ {
			f, err := os.Open(tc.mntDir)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = f.Readdirnames(-1)
			if err != nil {
				t.Errorf("goroutine %d iteration %d: %v", gr, i, err)
				f.Close()
				return
			}
			f.Close()
		}
	}

	n := 3
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go stress(i)
	}
	wg.Wait()
}

func TestMknod(t *testing.T) {
	tc := newTestCase(t, &testOptions{})
	defer tc.Clean()

	modes := map[string]uint32{
		"regular": syscall.S_IFREG,
		"socket":  syscall.S_IFSOCK,
		"fifo":    syscall.S_IFIFO,
	}

	for nm, mode := range modes {
		t.Run(nm, func(t *testing.T) {
			p := filepath.Join(tc.mntDir, nm)
			err := syscall.Mknod(p, mode|0755, (8<<8)|0)
			if err != nil {
				t.Fatalf("mknod(%s): %v", nm, err)
			}

			var st syscall.Stat_t
			if err := syscall.Stat(p, &st); err != nil {
				got := st.Mode &^ 07777
				if want := mode; got != want {
					t.Fatalf("stat(%s): got %o want %o", nm, got, want)
				}
			}

			// We could test if the files can be
			// read/written but: The kernel handles FIFOs
			// without talking to FUSE at all. Presumably,
			// this also holds for sockets.  Regular files
			// are tested extensively elsewhere.
		})
	}
}

func TestPosix(t *testing.T) {
	noisy := map[string]bool{
		"ParallelFileOpen": true,
		"ReadDir":          true,
	}

	for nm, fn := range posixtest.All {
		t.Run(nm, func(t *testing.T) {
			tc := newTestCase(t, &testOptions{
				suppressDebug: noisy[nm],
				attrCache:     true, entryCache: true})
			defer tc.Clean()

			fn(t, tc.mntDir)
		})
	}
}

func TestOpenDirectIO(t *testing.T) {
	// Apparently, tmpfs does not allow O_DIRECT, so try to create
	// a test temp directory in /var/tmp.
	ext4Dir, err := ioutil.TempDir("/var/tmp", "go-fuse.TestOpenDirectIO")
	if err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	defer os.RemoveAll(ext4Dir)

	posixtest.DirectIO(t, ext4Dir)
	if t.Failed() {
		t.Skip("DirectIO failed on underlying FS")
	}

	opts := testOptions{
		testDir:    ext4Dir,
		attrCache:  true,
		entryCache: true,
	}

	tc := newTestCase(t, &opts)
	defer tc.Clean()
	posixtest.DirectIO(t, tc.mntDir)
}

// FstatRenamed is similar to posixtest.FstatDeleted, but Fstat()s multiple renamed files
// in random order and checks that the results match an earlier Stat().
//
// Excercises the fd-finding logic in rawBridge.GetAttr.
func TestFstatRenamed(t *testing.T) {
	const iMax = 9
	type file struct {
		fd  int
		dst string
		s   syscall.Stat_t
		d   syscall.Stat_t
	}
	files := make(map[int]file)

	tc := newTestCase(t, &testOptions{attrCache: true, entryCache: true})
	defer tc.Clean()

	for i := 0; i <= iMax; i++ {
		// Create files with different sizes
		src := fmt.Sprintf("%s/%d.src", tc.mntDir, i)
		dst := fmt.Sprintf("%s/%d.dst", tc.mntDir, i)
		if err := ioutil.WriteFile(src, []byte(src), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		if err := ioutil.WriteFile(dst, []byte(dst), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		var d, s syscall.Stat_t
		if err := syscall.Stat(dst, &d); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Stat(src, &s); err != nil {
			t.Fatal(err)
		}

		// Open
		fd, err := syscall.Open(dst, syscall.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[i] = file{fd, dst, s, d}
		defer syscall.Close(fd)

		// Rename
		err = syscall.Rename(src, dst)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Fstat in random order
	for _, v := range files {
		var d, s syscall.Stat_t
		if err := syscall.Fstat(v.fd, &d); err != nil {
			t.Fatal(err)
		}

		if err := syscall.Stat(v.dst, &s); err != nil {
			t.Fatal(err)
		}
		// Ignore ctime, changes on unlink
		v.d.Ctim = syscall.Timespec{}
		d.Ctim = syscall.Timespec{}
		v.s.Ctim = syscall.Timespec{}
		s.Ctim = syscall.Timespec{}
		// Nlink value should have dropped to zero
		v.d.Nlink = 0
		// Rest should stay the same
		if v.d != d {
			t.Errorf("stat mismatch: want=%v\n have=%v", v.d, d)
		}

		if v.s != s {
			t.Errorf("stat mismatch: want=%v\n have=%v", v.s, s)
		}
	}
}

func TestRename2(t *testing.T) {
	tc := newTestCase(t, &testOptions{attrCache: true, entryCache: true})
	defer tc.Clean()

	//RENAME_EXCHANGE
	//RENAME_NOREPLACE
	//RENAME_WHITEOUT
	src := fmt.Sprintf("%s/src", tc.mntDir)
	dst := fmt.Sprintf("%s/dst", tc.mntDir)
	content1 := []byte(src)
	content2 := []byte(dst)
	if err := ioutil.WriteFile(src, content1, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_EXCHANGE); err == nil || err.(syscall.Errno) != syscall.ENOENT {
		t.Error(err)
	}

	if err := unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_NOREPLACE); err != nil {
		t.Error(err)
	}

	if data, err := ioutil.ReadFile(dst); err != nil || bytes.Compare(data, content1) != 0 {
		t.Fatalf("ReadAt: %v", err)
	}

	if err := ioutil.WriteFile(src, content2, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	err := unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_NOREPLACE)
	if err == nil || err.(syscall.Errno) != syscall.EEXIST {
		t.Error(err)
	}

	err = unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_EXCHANGE)
	if err != nil {
		t.Error(err)
	}

	if data, err := ioutil.ReadFile(dst); err != nil || bytes.Compare(data, content2) != 0 {
		t.Fatalf("ReadAt: %v", err)
	}

	if data, err := ioutil.ReadFile(src); err != nil || bytes.Compare(data, content1) != 0 {
		t.Fatalf("ReadAt: %v", err)
	}
}
