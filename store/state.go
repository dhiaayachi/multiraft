package store

import (
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/hashicorp/go-memdb"
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
	txn := p.db.Txn(true)
	pConf := &PartitionConfiguration{}
	err := encoding.DecodeMsgPack(log.Data, pConf)
	if err != nil {
		return err
	}
	err = txn.Insert(partitionTable, pConf)
	if err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	iter, err := s.txn.Get(partitionTable, indexID)
	if err != nil {
		return sink.Cancel()
	}
	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		b, err := encoding.EncodeMsgPack(entry.(*PartitionConfiguration))
		if err != nil {
			return sink.Cancel()
		}
		_, err = sink.Write(b.Bytes())
		if err != nil {
			return sink.Cancel()
		}
	}
	return sink.Close()
}

func (s *snapshot) Release() {
	s.txn.Abort()
}

func (p PartitionState) Snapshot() (raft.FSMSnapshot, error) {
	txn := p.db.Txn(false)
	return &snapshot{txn: txn}, nil

}

func (p PartitionState) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	panic("implement me")
}

type snapshot struct {
	txn *memdb.Txn
}
