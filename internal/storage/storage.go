package storage

type Storage struct {
	Pager *Pager
}

func NewStorage(filename string) (*Storage, error) {
	pager, err := NewPager(filename)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		Pager: pager,
	}

	return s, nil
}

func (s *Storage) Close() error {
	return s.Pager.Close()
}
