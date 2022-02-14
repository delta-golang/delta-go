package delta

import (
	"encoding/json"
	"fmt"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/schema"
	"github.com/delta-golang/delta-go/delta/storage"
	viewer "github.com/delta-golang/delta-go/delta/utils/parquet"
	"github.com/delta-golang/delta-go/delta/utils/slices"
	"os"
	"path/filepath"
)

const (
	LogDir             = "_delta_log"
	LastCheckPointFile = "_last_checkpoint"
	AddPath            = "add.path"
	RemovePath         = "remove.path"
	CDCPath            = "cdc.path"
	MetadataPath       = "metaData.id"
	TxnPath            = "txn.lastUpdated"
	ProtocolPath       = "protocol.minReaderVersion"
)

var ActionPaths = []string{AddPath, RemovePath, MetadataPath, CDCPath, TxnPath, ProtocolPath}

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

	paths := checkpointPathsForCheckpoint(t.lastCheckPoint)

	for _, v := range paths {
		p := filepath.Join(t.URI, v)
		rdr, err := file.OpenParquetFile(p, true)
		if err != nil {
			return err
		}

		seekers := make(map[index]*viewer.Seeker)

		for i := 0; i < rdr.NumRowGroups(); i++ {
			reader := rdr.RowGroup(i)
			idx := index{
				in: i,
			}
			createSeekers(rdr.MetaData().Schema, idx, reader, seekers)
		}

		rg := 0
		for i := 0; i < int(rdr.NumRows()); i++ {
			reader := rdr.RowGroup(rg)
			a, ok := findActionType(reader, index{in: rg}, seekers)
			if !ok {
				rg++
				continue
			}
			fmt.Println(a)
		}

		viewer.View(p)
	}
	return nil
}

type index struct {
	in   int
	path string
}

func createSeekers(schema *schema.Schema, idx index, reader *file.RowGroupReader, seekers map[index]*viewer.Seeker) {

	for _, a := range ActionPaths {
		i := schema.ColumnIndexByName(a)
		r := reader.Column(i)
		idx.path = a
		seekers[idx] = viewer.CreateDumper(r)
	}
}

func findActionType(reader *file.RowGroupReader, idx index, seekers map[index]*viewer.Seeker) (string, bool) {

	var action string
	for _, a := range ActionPaths {
		idx.path = a
		seeker := seekers[idx]
		if v, ok := seeker.Next(); !ok {
			return "", false
		} else {
			if v != nil && action == "" {
				action = a
			}
		}
	}
	return action, true
}

func (t *Table) updateIncrements() error {
	scanner, c, err := t.Storage.GetObject(commitPathForVersion(0))

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

func commitPathForVersion(version int) string {
	s := fmt.Sprintf("%020d.json", version)
	return filepath.Join(LogDir, s)
}

func checkpointPathsForCheckpoint(cp Checkpoint) []string {
	s := fmt.Sprintf("%020d", cp.Version)
	prefix := filepath.Join(LogDir, s)
	var paths []string
	if cp.Parts == 0 {
		paths = []string{prefix + ".checkpoint.parquet"}
	} else {
		for i := 0; i < int(cp.Parts); i++ {
			paths = append(paths, fmt.Sprintf("%s.checkpoint.%010d.%010d.parquet", prefix, i+1, cp.Parts))
		}
	}

	return paths
}

func (t *Table) logURI() string {
	return filepath.Join(t.URI, LogDir)
}
