package nfs

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
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

	tc.nfs, err = NewNFSRoot(tc.origDir, tc.store)
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

	time.Sleep(time.Second)

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
