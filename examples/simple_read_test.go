package examples

import (
	"github.com/delta-golang/delta-go/delta"
	"testing"
)

func TestSimpleRead(t *testing.T) {
	//_, err := delta.LoadTable("../tests/data/delta-0.8.0")
	_, err := delta.LoadTable("../tests/data/simple_table_with_checkpoint")
	if err != nil {
		t.Fatalf("could not load table: %s", err)
	}
}
