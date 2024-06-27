package store

import (
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"io"
)

//go:generate mockery --srcpkg=github.com/hashicorp/raft --name=SnapshotSink --inpackage

const indexID = "id"
const partitionTable = "partition"

type PartitionState struct {
	schema *memdb.DBSchema
	db     *memdb.MemDB
}

func NewPartitionState() (raft.FSM, error) {

	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			partitionTable: {
				Name: partitionTable,
				Indexes: map[string]*memdb.IndexSchema{
					indexID: {
						Name:         indexID,
						AllowMissing: false,
						Unique:       true,
						Indexer:      &memdb.UintFieldIndex{Field: "PartitionID"},
					},
				},
			},
		},
	}
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, err
	}
	return &PartitionState{
		schema: schema,
		db:     db,
	}, nil
}

func (p PartitionState) Apply(log *raft.Log) interface{} {

	pConf := &PartitionConfiguration{}
	err := encoding.DecodeMsgPack(log.Data, pConf)
	if err != nil {
		return err
	}
	return p.apply(pConf)
}

func (p PartitionState) apply(pConf *PartitionConfiguration) error {
	txn := p.db.Txn(true)
	err := txn.Insert(partitionTable, pConf)
	if err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {

	iter, err := s.txn.Get(partitionTable, indexID)
	if err != nil {
		_ = sink.Cancel()
		return err
	}

	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		b, err := encoding.EncodeMsgPack(entry.(*PartitionConfiguration))
		if err != nil {
			_ = sink.Cancel()
			return err
		}
		_, err = sink.Write(b.Bytes())
		if err != nil {
			_ = sink.Cancel()
			return err
		}
	}
	return sink.Close()
}

func (s *snapshot) Release() {
	// no need to abort the txn here as we create a read transaction
	// so aborting it is a noop
}

func (p PartitionState) Snapshot() (raft.FSMSnapshot, error) {
	txn := p.db.Txn(false)
	return &snapshot{txn: txn}, nil

}

func (p PartitionState) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(snapshot, &hd)
	conf := PartitionConfiguration{}
	var err error
	for err = dec.Decode(&conf); err == nil; {
		err = p.apply(&conf)
		if err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

type snapshot struct {
	txn *memdb.Txn
}
