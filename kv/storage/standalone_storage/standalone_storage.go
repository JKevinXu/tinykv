package standalone_storage

import (
	"io/ioutil"
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	dbPath, err := ioutil.TempDir("", "test-standalone")
	if err != nil {
		panic(err)
	}
	kvPath := filepath.Join(dbPath, "kv")
	kvDB := engine_util.CreateDB(kvPath, false)
	if kvDB == nil {
		return err
	}
	s.db = kvDB
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneReader{s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			engine_util.PutCF(s.db, data.Cf, data.Key, data.Value)
		case storage.Delete:
			engine_util.DeleteCF(s.db, data.Cf, data.Key)
		}
	}
	return nil
}

type StandAloneReader struct {
	standalonestorage *StandAloneStorage
}

func NewStandAloneReader(standalonestorage *StandAloneStorage) *StandAloneReader {
	return &StandAloneReader{
		standalonestorage: standalonestorage,
	}
}

func (sr *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(sr.standalonestorage.db, cf, key)
	if err != nil && err.Error() == "Key not found" {
		return nil, nil
	}
	return val, err
}

func (sr *StandAloneReader) Close() {
	sr.standalonestorage.Stop()
}

func (sr *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	txn := sr.standalonestorage.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}
