package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// SnapshotVersion 快照格式版本；不兼容的版本必须被拒绝。
const SnapshotVersion = 1

// Snapshot 已提交日志的压缩形态。
// 磁盘格式（大端序）：
//
//	version(8) | lastIncludedIndex(8) | lastIncludedTerm(8) | dataLen(8) | data | checksum(4)
//
// checksum 为对前面所有字节的 CRC32(IEEE)。
type Snapshot struct {
	Version           uint64 // 格式版本
	LastIncludedIndex uint64 // 快照覆盖的最大日志位点
	LastIncludedTerm  uint64 // 该位点的任期
	Data              []byte // 状态机数据
	Checksum          uint32 // 对编码内容（不含校验和自身）的 CRC32
}

// NewSnapshot 构造快照并计算校验和。
func NewSnapshot(lastIndex, lastTerm uint64, data []byte) *Snapshot {
	s := &Snapshot{
		Version:           SnapshotVersion,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              append([]byte(nil), data...),
	}
	s.Checksum = s.computeChecksum()
	return s
}

func (s *Snapshot) computeChecksum() uint32 {
	h := crc32.NewIEEE()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], s.Version)
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], s.LastIncludedIndex)
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], s.LastIncludedTerm)
	h.Write(buf[:])
	h.Write(s.Data)
	return h.Sum32()
}

// Valid 校验版本与校验和；返回分类错误便于调用方区分。
func (s *Snapshot) Valid() error {
	if s.Version != SnapshotVersion {
		return fmt.Errorf("%w: got %d want %d", ErrVersionMismatch, s.Version, SnapshotVersion)
	}
	if s.computeChecksum() != s.Checksum {
		return ErrSnapshotCorrupt
	}
	return nil
}

// Encode 序列化快照（含版本与校验和）。
func (s *Snapshot) Encode() ([]byte, error) {
	if err := s.Valid(); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 8*4+len(s.Data)+4)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], s.Version)
	out = append(out, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], s.LastIncludedIndex)
	out = append(out, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], s.LastIncludedTerm)
	out = append(out, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(len(s.Data)))
	out = append(out, buf[:]...)
	out = append(out, s.Data...)
	var c [4]byte
	binary.BigEndian.PutUint32(c[:], s.Checksum)
	out = append(out, c[:]...)
	return out, nil
}

// DecodeSnapshot 反序列化并校验版本与校验和；损坏或版本不符返回分类错误。
func DecodeSnapshot(b []byte) (*Snapshot, error) {
	if len(b) < 8*4+4 {
		return nil, fmt.Errorf("%w: too short (%d bytes)", ErrSnapshotCorrupt, len(b))
	}
	s := &Snapshot{}
	off := 0
	s.Version = binary.BigEndian.Uint64(b[off:])
	off += 8
	s.LastIncludedIndex = binary.BigEndian.Uint64(b[off:])
	off += 8
	s.LastIncludedTerm = binary.BigEndian.Uint64(b[off:])
	off += 8
	dataLen := binary.BigEndian.Uint64(b[off:])
	off += 8
	if uint64(len(b)) != uint64(off)+dataLen+4 {
		return nil, fmt.Errorf("%w: bad length field", ErrSnapshotCorrupt)
	}
	s.Data = append([]byte(nil), b[off:off+int(dataLen)]...)
	off += int(dataLen)
	s.Checksum = binary.BigEndian.Uint32(b[off:])
	if err := s.Valid(); err != nil {
		return nil, err
	}
	return s, nil
}
