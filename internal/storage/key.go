package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

type KeyType byte

const (
	KeyTypeInt     KeyType = 0x01
	KeyTypeText    KeyType = 0x02
	KeyTypeFloat   KeyType = 0x03
	KeyTypeBoolean KeyType = 0x04
)

type Key interface {
	Compare(other Key) int

	Encode() []byte

	Type() KeyType

	String() string
}

type IntKey struct {
	Value int64
}

func NewIntKey(value int64) *IntKey {
	return &IntKey{Value: value}
}

func (k *IntKey) Compare(other Key) int {
	otherInt, ok := other.(*IntKey)
	if !ok {

		if k.Type() < other.Type() {
			return -1
		}
		return 1
	}

	if k.Value < otherInt.Value {
		return -1
	} else if k.Value > otherInt.Value {
		return 1
	}
	return 0
}

func (k *IntKey) Encode() []byte {
	buf := make([]byte, 9)
	buf[0] = byte(KeyTypeInt)
	binary.BigEndian.PutUint64(buf[1:9], uint64(k.Value))
	return buf
}

func (k *IntKey) Type() KeyType {
	return KeyTypeInt
}

func (k *IntKey) String() string {
	return fmt.Sprintf("Int(%d)", k.Value)
}

type TextKey struct {
	Value string
}

func NewTextKey(value string) *TextKey {
	return &TextKey{Value: value}
}

func (k *TextKey) Compare(other Key) int {
	otherText, ok := other.(*TextKey)
	if !ok {

		if k.Type() < other.Type() {
			return -1
		}
		return 1
	}

	if k.Value < otherText.Value {
		return -1
	} else if k.Value > otherText.Value {
		return 1
	}
	return 0
}

func (k *TextKey) Encode() []byte {
	strBytes := []byte(k.Value)
	buf := make([]byte, 1+4+len(strBytes))
	buf[0] = byte(KeyTypeText)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(strBytes)))
	copy(buf[5:], strBytes)
	return buf
}

func (k *TextKey) Type() KeyType {
	return KeyTypeText
}

func (k *TextKey) String() string {
	return fmt.Sprintf("Text(%q)", k.Value)
}

type FloatKey struct {
	Value float64
}

func NewFloatKey(value float64) *FloatKey {
	return &FloatKey{Value: value}
}

func (k *FloatKey) Compare(other Key) int {
	otherFloat, ok := other.(*FloatKey)
	if !ok {

		if k.Type() < other.Type() {
			return -1
		}
		return 1
	}

	if k.Value < otherFloat.Value {
		return -1
	} else if k.Value > otherFloat.Value {
		return 1
	}
	return 0
}

func (k *FloatKey) Encode() []byte {
	buf := make([]byte, 9)
	buf[0] = byte(KeyTypeFloat)
	binary.BigEndian.PutUint64(buf[1:9], math.Float64bits(k.Value))
	return buf
}

func (k *FloatKey) Type() KeyType {
	return KeyTypeFloat
}

func (k *FloatKey) String() string {
	return fmt.Sprintf("Float(%f)", k.Value)
}

type BooleanKey struct {
	Value bool
}

func NewBooleanKey(value bool) *BooleanKey {
	return &BooleanKey{Value: value}
}

func (k *BooleanKey) Compare(other Key) int {
	otherBool, ok := other.(*BooleanKey)
	if !ok {

		if k.Type() < other.Type() {
			return -1
		}
		return 1
	}

	if !k.Value && otherBool.Value {
		return -1
	} else if k.Value && !otherBool.Value {
		return 1
	}
	return 0
}

func (k *BooleanKey) Encode() []byte {
	buf := make([]byte, 2)
	buf[0] = byte(KeyTypeBoolean)
	if k.Value {
		buf[1] = 1
	} else {
		buf[1] = 0
	}
	return buf
}

func (k *BooleanKey) Type() KeyType {
	return KeyTypeBoolean
}

func (k *BooleanKey) String() string {
	return fmt.Sprintf("Bool(%t)", k.Value)
}

func DecodeKey(data []byte) (Key, error) {
	if len(data) < 1 {
		return nil, errors.New("key data too short")
	}

	keyType := KeyType(data[0])

	switch keyType {
	case KeyTypeInt:
		if len(data) < 9 {
			return nil, errors.New("invalid int key data")
		}
		value := int64(binary.BigEndian.Uint64(data[1:9]))
		return NewIntKey(value), nil

	case KeyTypeText:
		if len(data) < 5 {
			return nil, errors.New("invalid text key data")
		}
		length := binary.BigEndian.Uint32(data[1:5])
		if len(data) < int(5+length) {
			return nil, errors.New("text key data truncated")
		}
		value := string(data[5 : 5+length])
		return NewTextKey(value), nil

	case KeyTypeFloat:
		if len(data) < 9 {
			return nil, errors.New("invalid float key data")
		}
		bits := binary.BigEndian.Uint64(data[1:9])
		value := math.Float64frombits(bits)
		return NewFloatKey(value), nil

	case KeyTypeBoolean:
		if len(data) < 2 {
			return nil, errors.New("invalid boolean key data")
		}
		value := data[1] != 0
		return NewBooleanKey(value), nil

	default:
		return nil, fmt.Errorf("unknown key type: %d", keyType)
	}
}

func KeysEqual(a, b Key) bool {
	return a.Compare(b) == 0
}

func KeysLess(a, b Key) bool {
	return a.Compare(b) < 0
}
