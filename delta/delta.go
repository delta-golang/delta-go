package delta

import (
	"errors"
	"fmt"
	"os"
)

func LoadTable(uri string) (*Table, error) {

	t := NewTable(uri)
	if t == nil {
		return nil, errors.New("could not create table")
	}

	cp, err := t.getLastCheckpoint()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	t.lastCheckPoint = cp
	err = t.restoreCheckpoint()
	if err != nil {
		// we don't want to abandon if a checkpoint is corrupted for any reason
		// we can still process the individual commits
		fmt.Printf("error restoring checkpoint with version: %d", cp.Version)
	}

	state, err := t.incrementalState(t.Version + 1)
	if err != nil {
		return nil, err
	}

	t.mergeState(state)
	return t, nil
}
