package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) (standstorage *StandAloneStorage) {
	// Your Code Here (1).
	DBPath := conf.DBPath
	KVPath := path.Join(DBPath, "kv")
	RaftPath := path.Join(DBPath, "Raft")
	KVengine := engine_util.CreateDB(KVPath, false)
	Raftengine := engine_util.CreateDB(RaftPath, true)
	standstorage = &StandAloneStorage{
		engines: engine_util.NewEngines(KVengine, Raftengine, KVPath, RaftPath),
	}
	return
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engines.Close()
	return nil
}

type BadgerStorageReader struct {
	txn *badger.Txn
}

func (bsr *BadgerStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, _ := engine_util.GetCFFromTxn(bsr.txn, cf, key)
	return value, nil
}

func (bsr *BadgerStorageReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, bsr.txn)
	return iter
}

func (bsr *BadgerStorageReader) Close() {
	bsr.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engines.Kv.NewTransaction(false)
	return &BadgerStorageReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writebatch := engine_util.WriteBatch{}
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			writebatch.SetCF(modify.Cf(), modify.Key(), modify.Value())

		case storage.Delete:
			writebatch.DeleteCF(modify.Cf(), modify.Key())
		}
	}
	return writebatch.WriteToDB(s.engines.Kv)
}
