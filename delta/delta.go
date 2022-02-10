package delta

import (
	"errors"
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
		return nil, err
	}

	err = t.updateIncrements()
	if err != nil {
		return nil, err
	}

	return t, nil
}
