package storage

import (
	"errors"
	"github.com/delta-golang/delta-go/delta/storage/file"
	"github.com/delta-golang/delta-go/delta/storage/s3"
	"strings"
)

type Backend interface {
	GetObject(uri string) ([]byte, error)
}

var (
	UnknownBackendError = errors.New("unknown backend type schema")
)

type BackendType int

const (
	BackendUnknownType BackendType = iota
	BackendFileType
	BackendS3Type
	BackendAzureType
	BackendGCPType
)

const (
	SchemaFile  = "file"
	SchemaS3    = "s3"
	SchemaAzure = "adls2"
	SchemaGCP   = "gs"
)

var (
	backendTypes = map[string]BackendType{
		SchemaFile:  BackendFileType,
		SchemaS3:    BackendS3Type,
		SchemaAzure: BackendAzureType,
		SchemaGCP:   BackendGCPType,
	}
)

type options struct {
	URI string
}

type Option func(*options)

func New(uri string, ops ...Option) Backend {

	os := &options{}
	for _, o := range ops {
		o(os)
	}

	path, t, err := parseBackend(uri)
	if err != nil {
		//log error
		return nil
	}

	var b Backend
	switch t {
	case BackendFileType:
		b = file.New(path)
	case BackendS3Type:
		b = s3.New(uri)
	default:
		return nil
	}

	return b
}

func parseBackend(uri string) (path string, t BackendType, err error) {

	p := strings.Split(uri, "://")
	if len(p) == 1 {
		return p[0], BackendFileType, nil
	}

	switch p[0] {
	case SchemaFile:
		return p[1], BackendFileType, nil
	case SchemaS3:
		return p[1], BackendS3Type, nil
	case SchemaGCP:
		return p[1], BackendGCPType, nil
	case SchemaAzure:
		return p[1], BackendAzureType, nil
	default:
		return "", BackendUnknownType, UnknownBackendError
	}
}
