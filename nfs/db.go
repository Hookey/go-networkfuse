package nfs

import (
	"fmt"
	"syscall"

	"github.com/timshannon/badgerhold/v4"
)

type MetaStore struct {
	Store *badgerhold.Store
}

func NewMetaStore(db string) (*MetaStore, error) {
	options := badgerhold.DefaultOptions
	options.Dir = db
	options.ValueDir = db
	store, err := badgerhold.Open(options)
	if err != nil {
		return nil, err
	}

	return &MetaStore{store}, nil
}

func (s *MetaStore) Close() {
	s.Store.Close()
}

func (s *MetaStore) Insert(pino uint64, name string, st *syscall.Stat_t, gen uint64) error {
	//TODO: reuse ino, increase gen
	i := Item{Ino: st.Ino, PIno: pino, Name: name, Stat: *st, Gen: gen}
	return s.Store.Upsert(st.Ino, i)
}

func (s *MetaStore) Getattr(ino uint64) (syscall.Stat_t, error) {
	var i Item
	err := s.Store.FindOne(&i, badgerhold.Where("Ino").Eq(ino))
	return i.Stat, err
}

func (s *MetaStore) Lookup(pino uint64, name string) (syscall.Stat_t, error) {
	var i Item
	err := s.Store.FindOne(&i, badgerhold.Where("PIno").Eq(pino).And("Name").Eq(name))
	return i.Stat, err
}

func (s *MetaStore) DeleteDentry(pino uint64, name string) error {
	err := s.Store.DeleteMatching(&Item{}, badgerhold.Where("PIno").Eq(pino).And("Name").Eq(name))
	return err
}

func (s *MetaStore) Delete(ino uint64) error {
	err := s.Store.Delete(ino, &Item{})
	return err
}

// SoftDelete moves certain inode to conceptual recycle bin(pino=0)
func (s *MetaStore) SoftDelete(ino uint64) error {
	return s.Store.UpdateMatching(&Item{}, badgerhold.Where("Ino").Eq(ino), func(record interface{}) error {
		i, ok := record.(*Item)
		if !ok {
			return fmt.Errorf("Record isn't the correct type!  Wanted Item, got %T", record)
		}

		i.PIno = RecycleBin
		return nil
	})
}

// ApplyIno returns grab one (Ino, Gen) pair from recycle bin to temp, so it won't be re-applied.
func (s *MetaStore) ApplyIno() (uint64, uint64) {
	var ino, gen uint64
	//TODO: test parallel applyino
	s.Store.UpdateMatching(&Item{}, badgerhold.Where("PIno").Eq(uint64(RecycleBin)).Limit(1), func(record interface{}) error {
		i, ok := record.(*Item)
		if !ok {
			return fmt.Errorf("Record isn't the correct type!  Wanted Item, got %T", record)
		}

		//log.Infof("apply %v", i)
		i.PIno = uint64(TempBin)
		ino, gen = i.Ino, i.Gen
		return nil
	})
	return ino, gen
}

// NextAllocateIno inspects the max number of inode, and returns its value adds one.
func (s *MetaStore) NextAllocateIno() uint64 {
	count, _ := s.Store.Count(&Item{}, nil)
	return count + 1
}

type Item struct {
	Ino  uint64 `badgerhold:"key"`
	PIno uint64
	Gen  uint64
	//Category string `badgerholdIndex:"Category"`
	//Created  time.Time
	Stat syscall.Stat_t
	Name string
	//Mtime syscall.Timespec
	//Mode  uint32
	//Uid   int
	//Gid   int
}
