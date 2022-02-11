package delta

import (
	"encoding/json"
	"fmt"
	"github.com/delta-golang/delta-go/delta/storage"
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
	Tombstones               map[string]RemoveAction
	Files                    []AddAction
	CommitInfos              []CommitInfo
	AppTransactionVersion    map[string]int64
	MinReaderVersion         int32
	MinWriterVersion         int32
	CurrentMetadata          Metadata
	TombstoneRetentionMillis int64
	LogRetentionMillis       int64
	EnableExpiredLogCleanup  bool
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
		var ac map[string]json.RawMessage
		err = json.Unmarshal(scanner.Bytes(), &ac)
		if err != nil {
			return err
		}

		for k, v := range ac {
			switch k {
			case "add":
				add, err := deserializeAction[AddAction](v)
				if err != nil {
					return err
				}
				t.State.Files = append(t.State.Files, add)
			case "remove":
				rm, err := deserializeAction[RemoveAction](v)
				if err != nil {
					return err
				}
				t.State.Tombstones[rm.Path] = rm
			case "metaData":
				md, err := deserializeAction[Metadata](v)
				if err != nil {
					return err
				}
				t.State.CurrentMetadata = md
			case "commitInfo":
				ci, err := deserializeAction[CommitInfo](v)
				if err != nil {
					return err
				}
				t.State.CommitInfos = append(t.State.CommitInfos, ci)
			case "protocol":
				p, err := deserializeAction[Protocol](v)
				if err != nil {
					return err
				}
				if p.MinReaderVersion > 0 {
					t.State.MinReaderVersion = p.MinReaderVersion
					t.State.MinWriterVersion = p.MinWriterVersion
				}
			}
		}
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
