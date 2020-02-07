package bloom

import (
	"encoding/binary"
	"math"
)

const CacheLineSize = 64
var Log2CacheLineSize = int(math.Log2(CacheLineSize))

func LegacyNoLocalityBloomImplAddHash(h uint32, totalBits uint32, numProbes int, data []byte) {
	delta := (h >> 17) | (h << 15)
	for i := 0; i < numProbes; i++ {
		bitpos := h % totalBits
		data[bitpos / 8] |= (1 << (bitpos % 8))
		h += delta
	}
}

func LegacyNoLocalityBloomImplHashMayMatch(h uint32, totalBits uint32, numProbes int, data []byte) bool {
	delta := (h >> 17) | (h << 15)
	for i := 0; i < numProbes; i++ {
		bitpos := h % totalBits
		if data[bitpos / 8] & (1 << (bitpos % 8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func LegacyLocalityBloomImplAddHash(h, numLines uint32, numProbes, log2CacheLineBytes int, data []byte) {
	log2CacheLineBits := log2CacheLineBytes + 3
	offset := (h % numLines) << uint(log2CacheLineBytes)
	delta := (h >> 17) | (h << 15)
	for i := 0; i < numProbes; i++ {
		bitpos := h & (uint32(1 << uint(log2CacheLineBits)) - 1)
		data[offset + bitpos / 8] |= byte(1 << uint(bitpos % 8))
		h += delta
	}
}

func LegacyLocalityBloomImplHashMayMatch(h uint32, numProbes, log2CacheLineBytes int, data []byte) bool {
	log2CacheLineBits := log2CacheLineBytes + 3

	delta := (h >> 17) | (h << 15)
	for i := 0; i < numProbes; i++ {
		bitpos := h & (uint32(1 << uint(log2CacheLineBits)) - 1)
		if data[bitpos / 8] & byte(1 << uint(bitpos % 8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func BloomHash(key []byte) uint32 {
	return Hash(key, 0xbc9f1d34)
}

type BloomFilterPolicy struct {
	bitsPerKey int
	numProbes  int
	hash       func([]byte) uint32
}

func NewBloomFilterPolicy(bitsPerKey int) *BloomFilterPolicy {
	numProbes := int(float64(bitsPerKey) * 0.69)
	if numProbes < 1 {
		numProbes = 1
	}
	if numProbes > 30 {
		numProbes = 30
	}
	return &BloomFilterPolicy{
		bitsPerKey: bitsPerKey,
		numProbes:  numProbes,
		hash:       BloomHash,
	}
}

func (p *BloomFilterPolicy) Name() string { return "rocksdb.BuiltinBloomFilter" }

func (p *BloomFilterPolicy) KeyMayMatch(key []byte, filter []byte) bool {
	l := len(filter)
	if l < 2 || l > 0xffffffff {
		return false
	}
	bits := uint32(l - 1) * 8
	if int(filter[l - 1]) > 30 {
		return true
	}
	return LegacyNoLocalityBloomImplHashMayMatch(p.hash(key), bits, int(filter[l - 1]), filter)
}

func (p *BloomFilterPolicy) GetFilterBitsBuilder() FilterBitsBuilder {
	return NewFullFilterBitsBuilder(p.bitsPerKey, p.numProbes)
}

func (p *BloomFilterPolicy) GetFilterBitsReader(contents []byte) FilterBitsReader {
	return NewFullFilterBitsReader(contents)
}

type FullFilterBitsBuilder struct {
	bitsPerKey  int
	numProbes   int
	hashEntries []uint32
}

func NewFullFilterBitsBuilder(bitsPerKey, numProbes int) *FullFilterBitsBuilder {
	if bitsPerKey <= 0 {
		return nil
	}
	return &FullFilterBitsBuilder{bitsPerKey: bitsPerKey, numProbes: numProbes}
}

func (b *FullFilterBitsBuilder) AddKey(key []byte) {
	hash := BloomHash(key)
	if len(b.hashEntries) == 0 || hash != b.hashEntries[len(b.hashEntries) - 1] {
		b.hashEntries = append(b.hashEntries, hash)
	}
}

func (b *FullFilterBitsBuilder) Finish() []byte {
	data, totalBits, numLines := b.reserveSpace(len(b.hashEntries))
	if totalBits > 0 && numLines > 0 {
		for _, h := range b.hashEntries {
			b.AddHash(h, numLines, totalBits, data)
		}
	}
	data[totalBits / 8] = byte(b.numProbes)
	binary.BigEndian.PutUint32(data[totalBits / 8 + 1:], numLines)

	b.hashEntries = b.hashEntries[:0]
	return data
}

func (b *FullFilterBitsBuilder) getTotalBitsForLocality(totalBits uint32) uint32 {
	numLines := (totalBits + CacheLineSize * 8 - 1) / (CacheLineSize * 8)
	if numLines % 2 == 0 {
		numLines++
	}
	return numLines * CacheLineSize * 8;
}

// (space bytes, total cache line bits, num of cache lines)
func (b *FullFilterBitsBuilder) CalculateSpace(numEntries int) (uint32, uint32, uint32) {
	var (
		totalBits uint32
		numLines  uint32
	)
	if numEntries > 0 {
		totalBits = b.getTotalBitsForLocality(uint32(numEntries * b.bitsPerKey))
		numLines = totalBits / (CacheLineSize * 8)
	}

	return totalBits / 8 + 5, totalBits, numLines
}

// space, total cache line bits, num of cache lines
func (b *FullFilterBitsBuilder) reserveSpace(numEntries int) ([]byte, uint32, uint32) {
	sz, totalBits, numLines := b.CalculateSpace(numEntries)
	return make([]byte, sz), totalBits, numLines
}

func (b *FullFilterBitsBuilder) AddHash(h, numLines, totalBits uint32, data []byte) {
	LegacyLocalityBloomImplAddHash(h, numLines, b.numProbes, Log2CacheLineSize, data)
}

type FullFilterBitsReader struct {
	data              []byte
	numProbes         int
	numLines          uint32
	log2CacheLineSize int
}

func NewFullFilterBitsReader(data []byte) *FullFilterBitsReader {
	if len(data) == 0 {
		return nil
	}

	var (
		numProbes         int
		numLines          uint32
		log2CacheLineSize int
	)

	// Get filter meta
	l := len(data)
	if l > 5 {
		numProbes = int(data[l - 5])
		numLines = binary.BigEndian.Uint32(data[l - 4:])
	}

	if numLines > 0 && uint32(l - 5) % numLines != 0 {
		numLines = 0
		numProbes = 0
	} else if numLines > 0 {
		for {
			tmp := uint32(l - 5) >> uint(log2CacheLineSize)
			if tmp == 0 {
				numLines = 0
				numProbes = 0
				break
			}
			if tmp == numLines {
				break
			}
			log2CacheLineSize++
		}
	}
	return &FullFilterBitsReader{
		data:              data,
		numProbes:         numProbes,
		numLines:          numLines,
		log2CacheLineSize: log2CacheLineSize,
	}
}

func (r *FullFilterBitsReader) KeyMayMatch(key []byte) bool {
	if len(r.data) <= 5 {
		return false
	}

	if r.numProbes == 0 || r.numLines == 0 {
		return true
	}

	hash := BloomHash(key)
	offset := (hash % r.numLines) << uint(r.log2CacheLineSize)
	return LegacyLocalityBloomImplHashMayMatch(hash, r.numProbes, r.log2CacheLineSize, r.data[offset:])
}

func (r *FullFilterBitsReader) KeysMayMatch(keys [][]byte) []bool {
	mayMatches := make([]bool, len(keys))
	if len(r.data) <= 5 {
		return mayMatches
	}

	for i := range mayMatches {
		mayMatches[i] = true
	}

	if r.numProbes == 0 || r.numLines == 0 {
		return mayMatches
	}

	hashes := make([]uint32, len(keys))
	offsets := make([]uint32, len(keys))
	for i := range keys {
		hashes[i] = BloomHash(keys[i])
		offsets[i] = (hashes[i] % r.numLines) << uint(r.log2CacheLineSize)
		if !LegacyLocalityBloomImplHashMayMatch(hashes[i], r.numProbes, r.log2CacheLineSize, r.data[offsets[i]:]) {
			mayMatches[i] = false
		}
	}
	return mayMatches
}