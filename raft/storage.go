package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// 磁盘文件与在线快照帧共用一套二进制格式：
//
//	magic(4) | version(1) | body... | crc32(4, little endian, 覆盖 magic..body)
//
// crc 不匹配或魔数/版本不符一律返回 KindSnapshotMismatch 类错误，
// 调用方必须显式处理，不得静默忽略。
const (
	magicState      = "RFST"
	magicEntries    = "RFEN"
	magicSnapshot   = "RFSN"
	magicCheckpoint = "RFCP"
	formatVersion   = byte(1)
	crcOverhead     = 4
)

func crcTable() *crc32.Table { return crc32.IEEETable }

// writeAtomicFile 先写临时文件并 fsync，再原子 rename，保证崩溃恢复时
// 要么看到旧文件、要么看到完整新文件，绝不会读到半写状态。
func writeAtomicFile(path string, data []byte) (err error) {
	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
			os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	// fsync 目录，保证 rename 落盘。
	if df, derr := os.Open(dir); derr == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

func readFileChecked(path, magic string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) < len(magic)+1+crcOverhead {
		return nil, raftErrorf(KindSnapshotMismatch, "file %s truncated (%d bytes)", path, len(raw))
	}
	if string(raw[:len(magic)]) != magic {
		return nil, raftErrorf(KindSnapshotMismatch, "file %s bad magic", path)
	}
	if raw[len(magic)] != formatVersion {
		return nil, raftErrorf(KindSnapshotMismatch,
			"file %s unsupported version %d (want %d)", path, raw[len(magic)], formatVersion)
	}
	bodyEnd := len(raw) - crcOverhead
	want := binary.LittleEndian.Uint32(raw[bodyEnd:])
	if got := crc32.Checksum(raw[:bodyEnd], crcTable()); got != want {
		return nil, raftErrorf(KindSnapshotMismatch, "file %s checksum mismatch (corrupt)", path)
	}
	return raw[len(magic)+1 : bodyEnd], nil
}

// frame 组装一个完整的带校验文件帧。
func frame(magic string, body []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(magic)
	buf.WriteByte(formatVersion)
	buf.Write(body)
	sum := crc32.Checksum(buf.Bytes(), crcTable())
	var sumB [crcOverhead]byte
	binary.LittleEndian.PutUint32(sumB[:], sum)
	buf.Write(sumB[:])
	return buf.Bytes()
}

func putU64(b *bytes.Buffer, v uint64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	b.Write(tmp[:])
}
func putU32(b *bytes.Buffer, v uint32) {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	b.Write(tmp[:])
}
func putU16(b *bytes.Buffer, v uint16) {
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], v)
	b.Write(tmp[:])
}
func putBytes(b *bytes.Buffer, p []byte) {
	putU32(b, uint32(len(p)))
	b.Write(p)
}

type byteReader struct{ b []byte }

func (r *byteReader) u64() (uint64, error) {
	if len(r.b) < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v, nil
}
func (r *byteReader) u32() (uint32, error) {
	if len(r.b) < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.LittleEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v, nil
}
func (r *byteReader) u16() (uint16, error) {
	if len(r.b) < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.LittleEndian.Uint16(r.b)
	r.b = r.b[2:]
	return v, nil
}
func (r *byteReader) bytes() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if uint32(len(r.b)) < n {
		return nil, io.ErrUnexpectedEOF
	}
	out := make([]byte, n)
	copy(out, r.b[:n])
	r.b = r.b[n:]
	return out, nil
}

// PersistentState 是每次变化都需 fsync 的 Raft 持久状态。
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
}

// Storage 抽象持久层，所有实现必须保证写入在返回前落盘。
type Storage interface {
	LoadState() (PersistentState, error)
	SaveState(PersistentState) error
	LoadSnapshot() (SnapshotMeta, []byte, bool, error)
	SaveSnapshot(meta SnapshotMeta, fsmData []byte) error
	LoadEntries() ([]Entry, error)
	// StoredLastIndex 返回当前持久化日志文件中的最大索引（无条目时为 0）。
	StoredLastIndex() uint64
	ReplaceEntries(firstIndex uint64, entries []Entry) error
	AppendEntries(entries []Entry) error
	CompactBefore(retainStart uint64) error
	io.Closer
}

// FileStorage 使用带 CRC32 的原子文件实现 Storage。
type FileStorage struct {
	dir     string
	entries []Entry // 启动时全量载入；变更后整文件原子重写
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{dir: dir}
	es, err := s.LoadEntries()
	if err != nil {
		return nil, err
	}
	s.entries = es
	return s, nil
}

func (s *FileStorage) statePath() string   { return filepath.Join(s.dir, "state.bin") }
func (s *FileStorage) entriesPath() string { return filepath.Join(s.dir, "entries.bin") }
func (s *FileStorage) snapshotPath() string {
	return filepath.Join(s.dir, "snapshot.bin")
}

func marshalState(st PersistentState) []byte {
	var b bytes.Buffer
	putU64(&b, st.CurrentTerm)
	putBytes(&b, []byte(st.VotedFor))
	return frame(magicState, b.Bytes())
}

func (s *FileStorage) LoadState() (PersistentState, error) {
	body, err := readFileChecked(s.statePath(), magicState)
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, err
	}
	r := byteReader{body}
	term, err := r.u64()
	if err != nil {
		return PersistentState{}, raftErrorf(KindInternal, "state body: %v", err)
	}
	vb, err := r.bytes()
	if err != nil {
		return PersistentState{}, raftErrorf(KindInternal, "state body: %v", err)
	}
	return PersistentState{CurrentTerm: term, VotedFor: string(vb)}, nil
}

func (s *FileStorage) SaveState(st PersistentState) error {
	return writeAtomicFile(s.statePath(), marshalState(st))
}

func marshalEntries(es []Entry) []byte {
	var b bytes.Buffer
	putU32(&b, uint32(len(es)))
	for _, e := range es {
		putU64(&b, e.Index)
		putU64(&b, e.Term)
		putBytes(&b, e.Data)
	}
	return frame(magicEntries, b.Bytes())
}

func unmarshalEntriesBody(body []byte) ([]Entry, error) {
	r := byteReader{body}
	n, err := r.u32()
	if err != nil {
		return nil, raftErrorf(KindSnapshotMismatch, "entries body: %v", err)
	}
	out := make([]Entry, 0, n)
	for i := uint32(0); i < n; i++ {
		idx, err := r.u64()
		if err != nil {
			return nil, raftErrorf(KindSnapshotMismatch, "entries body: %v", err)
		}
		term, err := r.u64()
		if err != nil {
			return nil, raftErrorf(KindSnapshotMismatch, "entries body: %v", err)
		}
		data, err := r.bytes()
		if err != nil {
			return nil, raftErrorf(KindSnapshotMismatch, "entries body: %v", err)
		}
		out = append(out, Entry{Index: idx, Term: term, Data: data})
	}
	return out, nil
}

func (s *FileStorage) LoadEntries() ([]Entry, error) {
	body, err := readFileChecked(s.entriesPath(), magicEntries)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return unmarshalEntriesBody(body)
}

func (s *FileStorage) StoredLastIndex() uint64 {
	if len(s.entries) == 0 {
		return 0
	}
	return s.entries[len(s.entries)-1].Index
}

func (s *FileStorage) persistEntries() error {
	return writeAtomicFile(s.entriesPath(), marshalEntries(s.entries))
}

func (s *FileStorage) AppendEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if len(s.entries) > 0 {
		last := s.entries[len(s.entries)-1].Index
		if entries[0].Index <= last {
			return raftErrorf(KindInternal,
				"append at %d would overwrite last index %d; use ReplaceEntries",
				entries[0].Index, last)
		}
	}
	s.entries = append(s.entries, entries...)
	return s.persistEntries()
}

func (s *FileStorage) ReplaceEntries(firstIndex uint64, entries []Entry) error {
	if len(entries) > 0 && entries[0].Index != firstIndex {
		return raftErrorf(KindInternal,
			"replace starts at %d, want %d", entries[0].Index, firstIndex)
	}
	s.entries = append([]Entry(nil), entries...)
	return s.persistEntries()
}

func (s *FileStorage) CompactBefore(retainStart uint64) error {
	kept := make([]Entry, 0, len(s.entries))
	for _, e := range s.entries {
		if e.Index >= retainStart {
			kept = append(kept, e)
		}
	}
	s.entries = kept
	return s.persistEntries()
}

// ---- 快照 ----

func marshalSnapshot(meta SnapshotMeta, fsmData []byte) []byte {
	var b bytes.Buffer
	putU64(&b, meta.LastIndex)
	putU64(&b, meta.LastTerm)
	putU16(&b, uint16(len(meta.Peers)))
	for _, p := range meta.Peers {
		putBytes(&b, []byte(p))
	}
	putBytes(&b, fsmData)
	return frame(magicSnapshot, b.Bytes())
}

func unmarshalSnapshot(raw []byte) (SnapshotMeta, []byte, error) {
	body, err := readFileCheckedBytes(raw, magicSnapshot)
	if err != nil {
		return SnapshotMeta{}, nil, err
	}
	r := byteReader{body}
	meta := SnapshotMeta{}
	if meta.LastIndex, err = r.u64(); err != nil {
		return meta, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
	}
	if meta.LastTerm, err = r.u64(); err != nil {
		return meta, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
	}
	np, err := r.u16()
	if err != nil {
		return meta, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
	}
	meta.Peers = make([]string, 0, np)
	for i := uint16(0); i < np; i++ {
		p, err := r.bytes()
		if err != nil {
			return meta, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
		}
		meta.Peers = append(meta.Peers, string(p))
	}
	fsmData, err := r.bytes()
	if err != nil {
		return meta, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
	}
	return meta, fsmData, nil
}

// readFileCheckedBytes 对内存中的整帧做校验（在线传输路径使用）。
func readFileCheckedBytes(raw []byte, magic string) ([]byte, error) {
	if len(raw) < len(magic)+1+crcOverhead {
		return nil, raftErrorf(KindSnapshotMismatch, "frame truncated (%d bytes)", len(raw))
	}
	if string(raw[:len(magic)]) != magic {
		return nil, raftErrorf(KindSnapshotMismatch, "bad magic in frame")
	}
	if raw[len(magic)] != formatVersion {
		return nil, raftErrorf(KindSnapshotMismatch,
			"unsupported frame version %d (want %d)", raw[len(magic)], formatVersion)
	}
	bodyEnd := len(raw) - crcOverhead
	want := binary.LittleEndian.Uint32(raw[bodyEnd:])
	if got := crc32.Checksum(raw[:bodyEnd], crcTable()); got != want {
		return nil, raftErrorf(KindSnapshotMismatch, "frame checksum mismatch (corrupt)")
	}
	return raw[len(magic)+1 : bodyEnd], nil
}

func (s *FileStorage) SaveSnapshot(meta SnapshotMeta, fsmData []byte) error {
	return writeAtomicFile(s.snapshotPath(), marshalSnapshot(meta, fsmData))
}

func (s *FileStorage) LoadSnapshot() (SnapshotMeta, []byte, bool, error) {
	raw, err := os.ReadFile(s.snapshotPath())
	if errors.Is(err, os.ErrNotExist) {
		return SnapshotMeta{}, nil, false, nil
	}
	if err != nil {
		return SnapshotMeta{}, nil, false, err
	}
	meta, data, err := unmarshalSnapshot(raw)
	if err != nil {
		return SnapshotMeta{}, nil, false, err
	}
	return meta, data, true, nil
}

// ReadRawSnapshot 返回磁盘上的完整快照帧（供在线传输，接收方做 CRC 校验）。
func (s *FileStorage) ReadRawSnapshot() ([]byte, error) {
	return os.ReadFile(s.snapshotPath())
}

func (s *FileStorage) Close() error { return nil }

// ---- 状态机检查点（fsm.bin）----

// FSMStore 持久化状态机已应用位点与数据，防止重启后重复应用。
type FSMStore interface {
	Load() (appliedIndex, appliedTerm uint64, data []byte, ok bool, err error)
	Save(appliedIndex, appliedTerm uint64, data []byte) error
	io.Closer
}

type fileFSMStore struct {
	path string
}

func newFileFSMStore(path string) (*fileFSMStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return &fileFSMStore{path: path}, nil
}

// checkpointMarshal 生成在线/落盘共用的检查点帧：
// magic | version | appliedIndex u64 | appliedTerm u64 | data... | crc32
func checkpointMarshal(appliedIndex, appliedTerm uint64, data []byte) []byte {
	var b bytes.Buffer
	putU64(&b, appliedIndex)
	putU64(&b, appliedTerm)
	b.Write(data)
	return frame(magicCheckpoint, b.Bytes())
}

func checkpointUnmarshal(raw []byte) (uint64, uint64, []byte, error) {
	body, err := readFileCheckedBytes(raw, magicCheckpoint)
	if err != nil {
		return 0, 0, nil, err
	}
	r := byteReader{body}
	idx, err := r.u64()
	if err != nil {
		return 0, 0, nil, raftErrorf(KindSnapshotMismatch, "checkpoint body: %v", err)
	}
	term, err := r.u64()
	if err != nil {
		return 0, 0, nil, raftErrorf(KindSnapshotMismatch, "checkpoint body: %v", err)
	}
	return idx, term, r.b, nil
}

func (s *fileFSMStore) Load() (uint64, uint64, []byte, bool, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil, false, nil
	}
	if err != nil {
		return 0, 0, nil, false, err
	}
	idx, term, data, err := checkpointUnmarshal(raw)
	if err != nil {
		return 0, 0, nil, false, err
	}
	return idx, term, data, true, nil
}

func (s *fileFSMStore) Save(appliedIndex, appliedTerm uint64, data []byte) error {
	return writeAtomicFile(s.path, checkpointMarshal(appliedIndex, appliedTerm, data))
}

func (s *fileFSMStore) Close() error { return nil }
