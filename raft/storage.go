package raft

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

var ErrCorruptStorage = errors.New("raft: corrupt persistent state")

const (
	storageMagic   uint32 = 0x52414654
	storageVersion uint32 = 1
	maxEntryBytes         = 16 << 20
)

type PersistentState struct {
	Term     Term
	VotedFor *NodeID
	Entries  []LogEntry
}

type Storage interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type FileStorage struct {
	path string
}

func NewFileStorage(path string) *FileStorage {
	return &FileStorage{path: path}
}

func (s *FileStorage) Load() (PersistentState, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, err
	}
	return decodeState(data)
}

func (s *FileStorage) Save(st PersistentState) error {
	buf := encodeState(st)
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	ok = true
	return syncDir(filepath.Dir(s.path))
}

// WHY: rename is atomic; this fsync commits the directory entry across power loss.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func encodeState(st PersistentState) []byte {
	n := 4 + 4 + 4 + 8 + 1 + 8 + 4
	for _, e := range st.Entries {
		n += 8 + 8 + 4 + len(e.Data)
	}
	buf := make([]byte, n)
	binary.LittleEndian.PutUint32(buf[0:4], storageMagic)
	binary.LittleEndian.PutUint32(buf[4:8], storageVersion)
	off := 12
	binary.LittleEndian.PutUint64(buf[off:], uint64(st.Term))
	off += 8
	if st.VotedFor != nil {
		buf[off] = 1
		binary.LittleEndian.PutUint64(buf[off+1:], uint64(*st.VotedFor))
	}
	off += 9
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(st.Entries)))
	off += 4
	for _, e := range st.Entries {
		binary.LittleEndian.PutUint64(buf[off:], uint64(e.Term))
		binary.LittleEndian.PutUint64(buf[off+8:], e.Index)
		binary.LittleEndian.PutUint32(buf[off+16:], uint32(len(e.Data)))
		off += 20
		copy(buf[off:], e.Data)
		off += len(e.Data)
	}
	sum := crc32.ChecksumIEEE(buf[12:])
	binary.LittleEndian.PutUint32(buf[8:12], sum)
	return buf
}

func decodeState(data []byte) (PersistentState, error) {
	if len(data) < 12 ||
		binary.LittleEndian.Uint32(data[0:4]) != storageMagic ||
		binary.LittleEndian.Uint32(data[4:8]) != storageVersion ||
		crc32.ChecksumIEEE(data[12:]) != binary.LittleEndian.Uint32(data[8:12]) {
		return PersistentState{}, ErrCorruptStorage
	}
	off := 12
	if len(data) < off+21 {
		return PersistentState{}, ErrCorruptStorage
	}
	st := PersistentState{Term: Term(binary.LittleEndian.Uint64(data[off:]))}
	off += 8
	flag := data[off]
	vote := NodeID(binary.LittleEndian.Uint64(data[off+1:]))
	off += 9
	if flag == 1 {
		st.VotedFor = &vote
	} else if flag != 0 {
		return PersistentState{}, ErrCorruptStorage
	}
	n := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if n > 1<<20 {
		return PersistentState{}, ErrCorruptStorage
	}
	st.Entries = make([]LogEntry, 0, n)
	for i := uint32(0); i < n; i++ {
		if len(data) < off+20 {
			return PersistentState{}, ErrCorruptStorage
		}
		e := LogEntry{
			Term:  Term(binary.LittleEndian.Uint64(data[off:])),
			Index: binary.LittleEndian.Uint64(data[off+8:]),
		}
		sz := binary.LittleEndian.Uint32(data[off+16:])
		off += 20
		if sz > maxEntryBytes || len(data) < off+int(sz) {
			return PersistentState{}, ErrCorruptStorage
		}
		if sz > 0 {
			e.Data = append([]byte(nil), data[off:off+int(sz)]...)
		}
		off += int(sz)
		st.Entries = append(st.Entries, e)
	}
	if off != len(data) {
		return PersistentState{}, ErrCorruptStorage
	}
	return st, nil
}

func cloneVote(id *NodeID) *NodeID {
	if id == nil {
		return nil
	}
	v := *id
	return &v
}

func cloneEntries(in []LogEntry) []LogEntry {
	out := make([]LogEntry, len(in))
	for i, e := range in {
		out[i] = e
		if len(e.Data) > 0 {
			out[i].Data = append([]byte(nil), e.Data...)
		}
	}
	return out
}

// WHY: Raft Figure 2 requires currentTerm, votedFor, and log durable before RPC replies.
func (n *Node) persistLocked() error {
	if n.storage == nil {
		return nil
	}
	return n.storage.Save(PersistentState{
		Term:     n.term,
		VotedFor: cloneVote(n.votedFor),
		Entries:  cloneEntries(n.log.entries),
	})
}

func (n *Node) restorePersistent(st PersistentState) {
	n.term = st.Term
	n.votedFor = cloneVote(st.VotedFor)
	if len(st.Entries) == 0 {
		n.log = newRaftLog()
		return
	}
	n.log = &raftLog{entries: cloneEntries(st.Entries)}
}
