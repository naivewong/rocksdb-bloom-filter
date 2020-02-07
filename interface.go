package bloom

type FilterPolicy interface {
	Name() string
	KeyMayMatch([]byte, []byte) bool // key, filter
	GetFilterBitsBuilder() FilterBitsBuilder
	GetFilterBitsReader([]byte) FilterBitsReader
}

type FilterBitsBuilder interface {
	AddKey([]byte)
	Finish() []byte
}

type FilterBitsReader interface {
	KeyMayMatch([]byte) bool
	KeysMayMatch([][]byte) []bool
}