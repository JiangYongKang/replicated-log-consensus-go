package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
)

// SnapshotVersion 快照格式版本；不兼容的版本必须被拒绝。
// v2 在 v1 基础上内嵌成员配置 Configuration（成员变更随快照恢复）。
const SnapshotVersion = 2

// Snapshot 已提交日志的压缩形态。
// v2 磁盘格式（大端序）：
//
//	version(8) | lastIncludedIndex(8) | lastIncludedTerm(8) | dataLen(8) | data
//	| cfgLen(8) | cfgJSON | checksum(4)
//
// cfgJSON 为 Configuration 的 JSON 编码（cfgLen=0 表示无配置）。
// checksum 为对前面所有字节的 CRC32(IEEE)。
type Snapshot struct {
	Version           uint64         // 格式版本
	LastIncludedIndex uint64         // 快照覆盖的最大日志位点
	LastIncludedTerm  uint64         // 该位点的任期
	Data              []byte         // 状态机数据
	Config            *Configuration // 快照位点处生效的成员配置（v2）
	Checksum          uint32         // 对编码内容（不含校验和自身）的 CRC32
}

// NewSnapshot 构造不带成员配置的快照并计算校验和。
func NewSnapshot(lastIndex, lastTerm uint64, data []byte) *Snapshot {
	return NewSnapshotWithConfig(lastIndex, lastTerm, data, nil)
}

// NewSnapshotWithConfig 构造携带成员配置的快照并计算校验和。
func NewSnapshotWithConfig(lastIndex, lastTerm uint64, data []byte, cfg *Configuration) *Snapshot {
	s := &Snapshot{
		Version:           SnapshotVersion,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              append([]byte(nil), data...),
	}
	if cfg != nil {
		c := cloneConfig(*cfg)
		s.Config = &c
	}
	s.Checksum = crc32.ChecksumIEEE(s.marshalBody())
	return s
}

// marshalBody 序列化校验和之前的全部字节；Encode 与校验共用同一份布局。
func (s *Snapshot) marshalBody() []byte {
	cfgBytes := []byte{}
	if s.Config != nil {
		b, err := json.Marshal(s.Config)
		if err != nil {
			panic(fmt.Errorf("raft: encode snapshot config: %w", err))
		}
		cfgBytes = b
	}
	out := make([]byte, 0, 8*5+len(s.Data)+len(cfgBytes))
	var buf [8]byte
	put := func(v uint64) {
		binary.BigEndian.PutUint64(buf[:], v)
		out = append(out, buf[:]...)
	}
	put(s.Version)
	put(s.LastIncludedIndex)
	put(s.LastIncludedTerm)
	put(uint64(len(s.Data)))
	out = append(out, s.Data...)
	put(uint64(len(cfgBytes)))
	out = append(out, cfgBytes...)
	return out
}

// Valid 校验版本与校验和；返回分类错误便于调用方区分。
func (s *Snapshot) Valid() error {
	if s.Version != SnapshotVersion {
		return fmt.Errorf("%w: got %d want %d", ErrVersionMismatch, s.Version, SnapshotVersion)
	}
	if crc32.ChecksumIEEE(s.marshalBody()) != s.Checksum {
		return ErrSnapshotCorrupt
	}
	return nil
}

// Encode 序列化快照（含版本、配置与校验和）。
func (s *Snapshot) Encode() ([]byte, error) {
	if err := s.Valid(); err != nil {
		return nil, err
	}
	out := append([]byte(nil), s.marshalBody()...)
	var c [4]byte
	binary.BigEndian.PutUint32(c[:], s.Checksum)
	out = append(out, c[:]...)
	return out, nil
}

// DecodeSnapshot 反序列化并校验版本与校验和；损坏或版本不符返回分类错误。
func DecodeSnapshot(b []byte) (*Snapshot, error) {
	if len(b) < 8*5+4 {
		return nil, fmt.Errorf("%w: too short (%d bytes)", ErrSnapshotCorrupt, len(b))
	}
	s := &Snapshot{}
	off := 0
	get := func() (uint64, error) {
		if off+8 > len(b) {
			return 0, ErrSnapshotCorrupt
		}
		v := binary.BigEndian.Uint64(b[off:])
		off += 8
		return v, nil
	}
	var err error
	if s.Version, err = get(); err != nil {
		return nil, fmt.Errorf("%w: version", ErrSnapshotCorrupt)
	}
	if s.LastIncludedIndex, err = get(); err != nil {
		return nil, fmt.Errorf("%w: lastIndex", ErrSnapshotCorrupt)
	}
	if s.LastIncludedTerm, err = get(); err != nil {
		return nil, fmt.Errorf("%w: lastTerm", ErrSnapshotCorrupt)
	}
	dataLen, err := get()
	if err != nil {
		return nil, fmt.Errorf("%w: dataLen", ErrSnapshotCorrupt)
	}
	if uint64(off)+dataLen > uint64(len(b)) {
		return nil, fmt.Errorf("%w: bad data length", ErrSnapshotCorrupt)
	}
	s.Data = append([]byte(nil), b[off:off+int(dataLen)]...)
	off += int(dataLen)
	cfgLen, err := get()
	if err != nil {
		return nil, fmt.Errorf("%w: cfgLen", ErrSnapshotCorrupt)
	}
	if uint64(off)+cfgLen+4 != uint64(len(b)) {
		return nil, fmt.Errorf("%w: bad config length", ErrSnapshotCorrupt)
	}
	if cfgLen > 0 {
		cfg := &Configuration{}
		if err := json.Unmarshal(b[off:off+int(cfgLen)], cfg); err != nil {
			return nil, fmt.Errorf("%w: config: %v", ErrSnapshotCorrupt, err)
		}
		s.Config = cfg
		off += int(cfgLen)
	} else {
		off += int(cfgLen)
	}
	s.Checksum = binary.BigEndian.Uint32(b[off:])
	if err := s.Valid(); err != nil {
		return nil, err
	}
	return s, nil
}
