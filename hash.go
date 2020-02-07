package bloom

import (
	"encoding/binary"
)

const hashm = 0xc6a4a793
const hashr = 24

func Hash(data []byte, seed uint32) uint32 {
	var h uint32
	h = seed ^ (uint32(len(data)) * hashm)
	cur := 0
	for cur + 4 <= len(data) {
		w := binary.BigEndian.Uint32(data[cur:])
		cur += 4
		h += w
		h *= hashm
		h ^= (h >> 16)
	}

	remain := len(data) - cur
	if remain == 3 {
		h += (uint32(data[cur + 2]) << 16)
		remain--
	}
	if remain == 2 {
		h += (uint32(data[cur + 1]) << 8)
		remain--
	}
	if remain == 1 {
		h += uint32(data[cur])
		h *= hashm
		h ^= (h >> hashr)
	}
	return h
}