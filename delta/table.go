package delta

import (
	"encoding/json"
	"fmt"
	"github.com/delta-golang/delta-go/delta/storage"
	"github.com/google/uuid"
	"log"
	"os"
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

func NewTable(uri string) *Table {

	s := storage.New(uri)
	if s == nil {
		return nil
	}

	t := Table{
		Storage: s,
		URI:     uri,
		Version: -1,
	}

	return &t
}

func (t *Table) getLastCheckpoint() (Checkpoint, error) {
	path := filepath.Join(LogDir, LastCheckPointFile)
	scanner, close, err := t.Storage.GetObject(path)

	if err != nil {
		return Checkpoint{}, err
	}

	defer close()

	if ok := scanner.Scan(); !ok {
		return Checkpoint{}, os.ErrNotExist
	}

	var cp Checkpoint
	err = json.Unmarshal(scanner.Bytes(), &cp)
	if err != nil {
		return Checkpoint{}, err
	}

	return cp, nil
}

func (t *Table) restoreCheckpoint() error {

	cp := Checkpoint{}
	if t.lastCheckPoint == cp {
		// no checkpoint
		return nil
	}

	return nil
}

func (t *Table) updateIncrements() error {
	scanner, close, err := t.Storage.GetObject(t.commitPathForVersion(0))
	defer close()

	if err != nil {
		return err
	}

	for scanner.Scan() {
		log.Println(scanner.Text())
	}
	return nil
}

func (t *Table) commitPathForVersion(version int) string {
	s := fmt.Sprintf("%020d.json", version)
	return filepath.Join(LogDir, s)
}

func (t *Table) logURI() string {
	return filepath.Join(t.URI, LogDir)
}
