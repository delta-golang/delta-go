package file

import (
	"io"
	"io/ioutil"
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

func (s *Store) GetObject(relativePath string) ([]byte, error) {

	p := filepath.Join(s.path, relativePath)
	data, err := ioutil.ReadFile(p)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return data, nil
}
