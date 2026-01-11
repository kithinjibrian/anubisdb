package storage

import "errors"

var (
	ErrPageFull      = errors.New("page is full")
	ErrInvalidSlot   = errors.New("invalid slot number")
	ErrTableNotFound = errors.New("table not found")
)
