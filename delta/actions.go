package delta

import "github.com/xitongsys/parquet-go/parquet"

type Action struct {
	path            string
	timestamp       int64
	dataChange      bool
	partitionValues map[string]string
	size            int64
	tags            map[string]string
}

type RemoveAction struct {
	Action
	extendedFileMetadata bool
}

type AddAction struct {
	Action
	partitionValuesParsed parquet.RowGroup
	stats                 string
	statsParsed           parquet.RowGroup
}

type ActionFormat struct {
	provider string
	options  map[string]string
}
