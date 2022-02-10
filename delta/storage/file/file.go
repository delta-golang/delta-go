package file

import (
	"bufio"
	"log"
	"os"
	"path/filepath"
)

type Store struct {
	path string
}

func New(uri string) *Store {

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("unable to get working directory: %s", err)
		return nil
	}

	path := filepath.Join(wd, uri)

	s := Store{
		path: path,
	}
	return &s
}

func (s *Store) GetObject(relativePath string) (*bufio.Scanner, func() error, error) {

	p := filepath.Join(s.path, relativePath)
	file, err := os.Open(p)
	if err != nil {
		return nil, nil, err
	}

	c := func() error {
		return file.Close()
	}

	scanner := bufio.NewScanner(file)
	return scanner, c, nil
}
