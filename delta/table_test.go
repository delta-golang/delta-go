package delta

import (
	"github.com/delta-golang/delta-go/delta/storage"
	"testing"
)

func TestGetLastCheckpoint(t *testing.T) {
	s := storage.New("../tests/data/simple_table_with_checkpoint")
	tbl := Table{Storage: s}
	cp, err := tbl.getLastCheckpoint()
	if err != nil {
		t.Errorf("error getting checkpoint: %s", err)
	}

	if cp.Version != 10 || cp.Size != 13 {
		t.Errorf("incorrect version or size")
	}
}
