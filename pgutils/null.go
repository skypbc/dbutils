package pgutils

import (
	"fmt"

	"github.com/google/uuid"
)

type NullBytea struct {
	Bytes []byte
	Valid bool
}

func (nb *NullBytea) Scan(value any) error {
	if value == nil {
		nb.Bytes, nb.Valid = nil, false
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf(`expected []byte, got "%T"`, value)
	}
	nb.Bytes, nb.Valid = b, true
	return nil
}

type NullUUID struct {
	UUID  uuid.UUID
	Valid bool
}

func (n *NullUUID) Scan(value interface{}) error {
	if value == nil {
		n.UUID, n.Valid = uuid.UUID{}, false
		return nil
	}
	switch v := value.(type) {
	case []byte:
		u, err := uuid.ParseBytes(v)
		if err != nil {
			return err
		}
		n.UUID, n.Valid = u, true
		return nil
	case string:
		u, err := uuid.Parse(v)
		if err != nil {
			return err
		}
		n.UUID, n.Valid = u, true
		return nil
	default:
		return fmt.Errorf(`cannot scan type "%T" into NullUUID`, value)
	}
}
