package delta

import (
	"encoding/json"
	"github.com/delta-golang/delta-go/delta/storage"
	"github.com/google/uuid"
	"path/filepath"
)

const (
	LogDir             = "_delta_log"
	LastCheckPointFile = "_last_checkpoint"
)

type Table struct {
	Storage          storage.Backend
	Options          TableOptions
	Version          int64
	VersionTimestamp int64
	URI              string
	BackendType      storage.BackendType
	State            TableState
	lastCheckPoint   Checkpoint
}

type TableOptions struct {
	requiresTombstones bool
}

type TableState struct {
	tombstones               map[string]RemoveAction
	files                    []AddAction
	commitInfos              []map[string]interface{}
	appTransactionVersion    map[string]int64
	minReaderVersion         int32
	minWriterVersion         int32
	currentMetadata          Metadata
	tombstoneRetentionMillis int64
	logRetentionMillis       int64
	enableExpiredLogCleanup  bool
}

type Metadata struct {
	id               uuid.UUID
	name             string
	description      string
	actionFormat     ActionFormat
	schema           string
	partitionColumns []string
	timestamp        int64
	config           map[string]string
}

type Checkpoint struct {
	Version int64 //20 digit decimals
	Size    int64
	Parts   uint32 //10 digit decimals
}

func (t *Table) getLastCheckpoint() (Checkpoint, error) {
	path := filepath.Join(LogDir, LastCheckPointFile)
	data, err := t.Storage.GetObject(path)
	if err != nil {
		return Checkpoint{}, err
	}

	var cp Checkpoint
	err = json.Unmarshal(data, &cp)
	if err != nil {
		return Checkpoint{}, err
	}

	return cp, nil
}

func (t *Table) logURI() string {
	return filepath.Join(t.URI, LogDir)
}
