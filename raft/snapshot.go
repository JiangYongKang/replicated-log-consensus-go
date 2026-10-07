package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// SnapshotVersion 快照格式版本；不兼容的版本必须被拒绝。
// v2 起快照携带成员配置（成员变更后重启可推导出唯一配置）。
const SnapshotVersion = 2

// Snapshot 已提交日志的压缩形态。
// 磁盘格式（大端序）：
//
//	version(8) | lastIncludedIndex(8) | lastIncludedTerm(8) | dataLen(8) | data |
//	voterCount(8) | voter(len(8)+bytes)... | learnerCount(8) | learner(len(8)+bytes)... |
//	checksum(4)
//
// checksum 为对前面所有字节的 CRC32(IEEE)。
type Snapshot struct {
	Version           uint64   // 格式版本
	LastIncludedIndex uint64   // 快照覆盖的最大日志位点
	LastIncludedTerm  uint64   // 该位点的任期
	Data              []byte   // 状态机数据
	Voters            []NodeID // 快照点的投票成员（排序存储）
	Learners          []NodeID // 快照点的学习者（排序存储）
	Checksum          uint32   // 对编码内容（不含校验和自身）的 CRC32
}

// NewSnapshot 构造快照并计算校验和；成员列表会被排序拷贝。
func NewSnapshot(lastIndex, lastTerm uint64, data []byte, voters, learners []NodeID) *Snapshot {
	s := &Snapshot{
		Version:           SnapshotVersion,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              append([]byte(nil), data...),
		Voters:            append([]NodeID(nil), voters...),
		Learners:          append([]NodeID(nil), learners...),
	}
	s.Checksum = s.computeChecksum()
	return s
}

// encodeIDList 编码节点列表：count(8) + 每项 len(8)+bytes。
func encodeIDList(ids []NodeID) []byte {
	out := make([]byte, 0, 8)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(len(ids)))
	out = append(out, buf[:]...)
	for _, id := range ids {
		binary.BigEndian.PutUint64(buf[:], uint64(len(id)))
		out = append(out, buf[:]...)
		out = append(out, id...)
	}
	return out
}

// decodeIDList 解码节点列表；返回剩余字节。
func decodeIDList(b []byte) ([]NodeID, []byte, error) {
	if len(b) < 8 {
		return nil, nil, fmt.Errorf("%w: id list truncated", ErrSnapshotCorrupt)
	}
	count := binary.BigEndian.Uint64(b)
	b = b[8:]
	ids := make([]NodeID, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(b) < 8 {
			return nil, nil, fmt.Errorf("%w: id length truncated", ErrSnapshotCorrupt)
		}
		l := binary.BigEndian.Uint64(b)
		b = b[8:]
		if uint64(len(b)) < l {
			return nil, nil, fmt.Errorf("%w: id bytes truncated", ErrSnapshotCorrupt)
		}
		ids = append(ids, NodeID(b[:l]))
		b = b[l:]
	}
	return ids, b, nil
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
	h.Write(encodeIDList(s.Voters))
	h.Write(encodeIDList(s.Learners))
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

// Encode 序列化快照（含版本、成员配置与校验和）。
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
	out = append(out, encodeIDList(s.Voters)...)
	out = append(out, encodeIDList(s.Learners)...)
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
	if uint64(len(b)) < uint64(off)+dataLen {
		return nil, fmt.Errorf("%w: bad length field", ErrSnapshotCorrupt)
	}
	s.Data = append([]byte(nil), b[off:off+int(dataLen)]...)
	off += int(dataLen)
	rest := b[off:]
	var err error
	s.Voters, rest, err = decodeIDList(rest)
	if err != nil {
		return nil, err
	}
	s.Learners, rest, err = decodeIDList(rest)
	if err != nil {
		return nil, err
	}
	if len(rest) != 4 {
		return nil, fmt.Errorf("%w: bad length field", ErrSnapshotCorrupt)
	}
	s.Checksum = binary.BigEndian.Uint32(rest)
	if err := s.Valid(); err != nil {
		return nil, err
	}
	return s, nil
}
