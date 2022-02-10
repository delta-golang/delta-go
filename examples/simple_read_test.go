package examples

import (
	"github.com/delta-golang/delta-go/delta/storage"
	"testing"
)

func TestSimpleRead(t *testing.T) {
	s := storage.New("../tests/data/delta-0.8.0")
	s.GetObject("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet")
}
