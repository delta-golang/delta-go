package delta

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go/parquet"
)

type Action struct {
	Path            string
	DataChange      bool
	PartitionValues map[string]string
	Size            int64
	Tags            map[string]string
}

type RemoveAction struct {
	Action
	DeletionTimestamp    int64
	ExtendedFileMetadata bool
}

type AddAction struct {
	Action
	ModificationTime      int64
	PartitionValuesParsed parquet.RowGroup
	Stats                 string
	StatsParsed           parquet.RowGroup
}

type ActionFormat struct {
	Provider string
	Options  map[string]string
}

type Metadata struct {
	ID               uuid.UUID
	Name             string
	Description      string
	Format           ActionFormat
	SchemaString     string
	PartitionColumns []string
	CreatedTime      int64
	Configuration    map[string]string
}

type Protocol struct {
	MinReaderVersion int32
	MinWriterVersion int32
}

type CommitInfo map[string]interface{}

type ActionTypes interface {
	AddAction | RemoveAction | Metadata | CommitInfo | Protocol
}

func deserializeAction[T ActionTypes](b []byte) (T, error) {
	var action T
	err := json.Unmarshal(b, &action)
	return action, err
}
