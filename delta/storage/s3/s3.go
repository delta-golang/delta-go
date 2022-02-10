package s3

import (
	"bufio"
)

type Store struct {
	Bucket    string
	Key       string
	storeType string
}

func New(uri string) *Store {

	return nil
}

func (s *Store) GetObject(uri string) (*bufio.Scanner, func() error, error) {
	//TODO implement me
	panic("implement me")
}
