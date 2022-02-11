package delta

import (
	"encoding/json"
	"fmt"
	"github.com/delta-golang/delta-go/delta/storage"
	"github.com/delta-golang/delta-go/delta/utils/slices"
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
	scanner, c, err := t.Storage.GetObject(path)

	if err != nil {
		return Checkpoint{}, err
	}
	defer c()

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
	scanner, c, err := t.Storage.GetObject(t.commitPathForVersion(0))

	if err != nil {
		return err
	}
	defer c()

	newState := TableState{}
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
				newState.Files = append(t.State.Files, add)
			case "remove":
				rm, err := deserializeAction[RemoveAction](v)
				if err != nil {
					return err
				}
				newState.Tombstones[rm.Path] = rm
			case "metaData":
				md, err := deserializeAction[Metadata](v)
				if err != nil {
					return err
				}
				newState.CurrentMetadata = md
			case "commitInfo":
				ci, err := deserializeAction[CommitInfo](v)
				if err != nil {
					return err
				}
				newState.CommitInfos = append(t.State.CommitInfos, ci)
			case "protocol":
				p, err := deserializeAction[Protocol](v)
				if err != nil {
					return err
				}
				newState.MinReaderVersion = p.MinReaderVersion
				newState.MinWriterVersion = p.MinWriterVersion
			}
		}
	}
	t.mergeState(&newState)
	return nil
}

func (t *Table) mergeState(s *TableState) {

	// remove files from the table that have a tombstone in new state
	t.State.Files = slices.Filter(t.State.Files, func(f AddAction) bool {
		for _, v := range s.Tombstones {
			if f.Path == v.Path {
				return false
			}
		}
		return true
	})

	// add all new tombstones
	for k, v := range s.Tombstones {
		t.State.Tombstones[k] = v
	}

	// remove from tombstones the ones that have a new file
	for _, v := range s.Files {
		delete(t.State.Tombstones, v.Path)
	}

	t.State.Files = append(t.State.Files, s.Files...)
}

func (t *Table) commitPathForVersion(version int) string {
	s := fmt.Sprintf("%020d.json", version)
	return filepath.Join(LogDir, s)
}

func (t *Table) logURI() string {
	return filepath.Join(t.URI, LogDir)
}
