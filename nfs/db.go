package nfs

import (
	"fmt"
	"syscall"

	"github.com/dgraph-io/badger/v3"
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

func (s *MetaStore) Insert(pino uint64, name string, st *syscall.Stat_t, gen uint64, target string) error {
	i := Item{Ino: st.Ino, Link: Link_t{Pino: pino, Name: name}, Stat: *st, Gen: gen, Target: target}
	return s.Store.Upsert(st.Ino, i)
}

func (s *MetaStore) Lookup(ino uint64) *Item {
	var i Item
	s.Store.FindOne(&i, badgerhold.Where("Ino").Eq(ino))
	return &i
}

func (s *MetaStore) LookupDentry(pino uint64, name string) *Item {
	var i Item
	s.Store.FindOne(&i, badgerhold.Where("Link.Pino").Eq(pino).And("Link.Name").Eq(name))
	return &i
}

func (s *MetaStore) DeleteDentry(pino uint64, name string) error {
	err := s.Store.DeleteMatching(&Item{}, badgerhold.Where("Link.Pino").Eq(pino).And("Name").Eq(name))
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

		i.Link.Pino = RecycleBin
		return nil
	})
}

// ApplyIno returns grab one (Ino, Gen) pair from recycle bin to temp, so it won't be re-applied.
func (s *MetaStore) ApplyIno() (uint64, uint64) {
	var ino, gen uint64
	//TODO: test parallel applyino
	s.Store.UpdateMatching(&Item{}, badgerhold.Where("Link.Pino").Eq(uint64(RecycleBin)).Limit(1), func(record interface{}) error {
		i, ok := record.(*Item)
		if !ok {
			return fmt.Errorf("Record isn't the correct type!  Wanted Item, got %T", record)
		}

		//log.Infof("apply %v", i)
		i.Link.Pino = uint64(TempBin)
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

func (s *MetaStore) IsEmptyDir(ino uint64) bool {
	count, err := s.Store.Count(&Item{}, badgerhold.Where("Link.Pino").Eq(ino).Limit(1))
	return count == 0 && err == nil
}

func (s *MetaStore) ReadDir(ino uint64) []*Item {
	is := []*Item{}
	s.Store.Find(&is, badgerhold.Where("Ino").Eq(ino).Or(badgerhold.Where("Link.Pino").Eq(ino)))
	return is
}

// Replace is a variant of rename from ino to ino2
func (s *MetaStore) Replace(ino, ino2, pino2 uint64, name2 string) error {
	return s.Store.Badger().Update(func(tx *badger.Txn) error {
		if err := s.Store.TxUpdateMatching(tx, &Item{}, badgerhold.Where("Ino").Eq(ino2), func(record interface{}) error {
			i, ok := record.(*Item)
			if !ok {
				return fmt.Errorf("Record isn't the correct type!  Wanted Item, got %T", record)
			}

			i.Link.Pino = RecycleBin
			return nil
		}); err != nil {
			return err
		}

		return s.Store.TxUpdateMatching(tx, &Item{}, badgerhold.Where("Ino").Eq(ino), func(record interface{}) error {
			i, ok := record.(*Item)
			if !ok {
				return fmt.Errorf("Record isn't the correct type!  Wanted Item, got %T", record)
			}

			//log.Infof("apply %v", i)
			i.Link.Pino = pino2
			i.Link.Name = name2
			return nil
		})
	})
}

func (s *MetaStore) Rename(ino, pino2 uint64, name2 string) error {
	return s.Store.UpdateMatching(&Item{}, badgerhold.Where("Ino").Eq(ino), func(record interface{}) error {
		i, ok := record.(*Item)
		if !ok {
			return fmt.Errorf("Record isn't the correct type!  Wanted Item, got %T", record)
		}

		//log.Infof("apply %v", i)
		i.Link.Pino = pino2
		i.Link.Name = name2
		return nil
	})
}

func (s *MetaStore) Setattr(ino uint64, st *syscall.Stat_t) error {
	return s.Store.UpdateMatching(&Item{}, badgerhold.Where("Ino").Eq(ino), func(record interface{}) error {
		i, ok := record.(*Item)
		if !ok {
			return fmt.Errorf("Record isn't the correct type!  Wanted Item, got %T", record)
		}

		i.Stat = *st
		return nil
	})
}

type Link_t struct {
	Pino uint64
	Name string
}

type Item struct {
	Ino  uint64 `badgerhold:"key"`
	Link Link_t
	//Pino uint64
	Gen uint64
	//Category string `badgerholdIndex:"Category"`
	//Created  time.Time
	Stat syscall.Stat_t
	//Name   string
	Target string
	//Mtime syscall.Timespec
	//Mode  uint32
	//Uid   int
	//Gid   int
}
