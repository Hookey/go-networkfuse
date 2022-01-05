package nfs

import (
	"fmt"
	"syscall"
	"testing"
)

func TestInsertLookup(t *testing.T) {
	tc := newTestCase(t, nil)
	defer tc.Clean()

	if err := quickInsert(tc.store, 1); err != nil {
		t.Fatalf("insert: %v", err)
	}

	i := tc.store.Lookup(2)
	if i.Ino != 2 {
		t.Fatalf("lookup: %v", i.Ino)
	}
}

// quickInsert works as inserting (pino, name) sequences of (1, "2"), (2, "3"), etc.
func quickInsert(ms *MetaStore, num int) (err error) {
	for i := 1; i <= num; i++ {
		ino := uint64(i + 1)
		pino := uint64(i)
		st := syscall.Stat_t{Ino: ino}
		st.X__unused[1] = Used
		name := fmt.Sprintf("%v", ino)
		err = ms.Insert(pino, name, &st, 0, "")
		if err != nil {
			return
		}
	}
	return
}

func TestResolve(t *testing.T) {
	tc := newTestCase(t, nil)
	defer tc.Clean()

	if err := quickInsert(tc.store, 4); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var tests = []struct {
		path string
		name string
		ino  uint64
	}{
		{"/2", "2", 2},
		{"/2/3", "3", 3},
		{"/2/3/4", "4", 4},
		{"//2///3//4////", "4", 4},
		{"/2/4", "", 0},
	}

	for _, tt := range tests {
		i := tc.store.Resolve(tt.path)
		if i.Dentry.Name != tt.name || i.Ino != tt.ino {
			t.Errorf("Resolve(%v) = %v,%v expected %v,%v",
				tt.path, i.Ino, i.Dentry.Name, tt.ino, tt.name)
		}
	}

}
