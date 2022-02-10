package s3

type Store struct {
	Bucket    string
	Key       string
	storeType string
}

func New(uri string) *Store {

	return nil
}

func (s *Store) GetObject(uri string) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}
