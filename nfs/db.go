package nfs

import (
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

func (s *MetaStore) Insert(pino uint64, name string, st *syscall.Stat_t) error {
	//TODO: reuse ino, increase gen
	i := Item{Ino: st.Ino, PIno: pino, Name: name, Stat: *st, Gen: 1}
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

func (s *MetaStore) SoftDelete(ino uint64) error {
	err := s.Store.ForEach(badgerhold.Where("Ino").Eq(ino), func(i *Item) error {
		i.PIno = 0
		return nil
	})
	return err
}

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
