package nfs

import (
	"fmt"
	"syscall"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v3"
	"github.com/timshannon/badgerhold/v4"
)

type MetaStore struct {
	Store *badgerhold.Store

	// hash function is used to index a link, boost up lookup performance.
	hash func(s string) uint64
}

func NewMetaStore(db string) (*MetaStore, error) {
	options := badgerhold.DefaultOptions
	options.Dir = db
	options.ValueDir = db
	store, err := badgerhold.Open(options)
	if err != nil {
		return nil, err
	}

	return &MetaStore{Store: store, hash: xxhash.Sum64String}, nil
}

func (s *MetaStore) Close() {
	s.Store.Close()
}

func (s *MetaStore) hashLink(pino uint64, name string) uint64 {
	// Add "/" to prevent hash collision, for instance, ("12", 3), ("1", 23).
	return s.hash(fmt.Sprint(name+"/", pino))
}

func (s *MetaStore) Insert(pino uint64, name string, st *syscall.Stat_t, gen uint64, target string) error {
	i := Item{Ino: st.Ino, Link: Link_t{Pino: pino, Name: name}, Hash: s.hashLink(pino, name), Stat: *st, Gen: gen, Target: target}
	return s.Store.Upsert(st.Ino, i)
}

func (s *MetaStore) Lookup(ino uint64) *Item {
	var i Item
	s.Store.FindOne(&i, badgerhold.Where("Ino").Eq(ino))
	return &i
}

func (s *MetaStore) LookupDentry(pino uint64, name string) *Item {
	var i Item
	h := s.hashLink(pino, name)
	//s.Store.FindOne(&i, badgerhold.Where("Hash").Eq(h).Index("hashIdx"))
	s.Store.ForEach(badgerhold.Where("Hash").Eq(h).Index("hashIdx"), func(record *Item) error {
		if record.Link.Pino == pino && record.Link.Name == name {
			i = *record
			return fmt.Errorf("Got it!")
		}

		return nil
	})
	return &i
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

func (s *MetaStore) DeleteDentry(pino uint64, name string) error {
	lnk := Link_t{Pino: pino, Name: name}
	//TODO: hardlink feature
	err := s.Store.DeleteMatching(&Item{}, badgerhold.Where("Link").Eq(lnk))
	return err
}

func (s *MetaStore) Delete(ino uint64) error {
	err := s.Store.Delete(ino, &Item{})
	return err
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

			i.Link.Pino = pino2
			i.Link.Name = name2
			i.Hash = s.hashLink(pino2, name2)
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

		i.Link.Pino = pino2
		i.Link.Name = name2
		i.Hash = s.hashLink(pino2, name2)
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
	Ino    uint64 `badgerhold:"key"`
	Gen    uint64
	Hash   uint64 `badgerholdIndex:"hashIdx"`
	Link   Link_t
	Stat   syscall.Stat_t
	Target string
}
