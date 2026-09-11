package replayrecovery

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// lockedDB uses a separate OS lock in addition to SQLite's transaction locks:
// the lock spans remote operations, while every journal commit is individually
// durable before the next remote mutation. Its directory must be private.
type lockedDB struct {
	db   *sql.DB
	file *os.File
}

func openPrivateDB(path string, create, readonly bool) (_ *lockedDB, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return nil, failure("recovery directory must be private (0700): %s", filepath.Dir(path))
	}
	flags := unix.O_RDWR | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if create {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	if readonly {
		flags = unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	}
	fd, err := unix.Open(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close() //nolint:errcheck // SQLite opens the validated inode by path below
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		return nil, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0077 != 0 || int64(stat.Uid) != int64(os.Geteuid()) {
		return nil, failure("recovery file must be an owned private regular file: %s", path)
	}
	// SQLite uses locks on its database inode. On macOS flock can interfere
	// with those locks, so hold the lifetime lock on a separate private inode.
	lockFD, err := unix.Open(path+".lock", unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	lockFile := os.NewFile(uintptr(lockFD), path+".lock")
	defer func() {
		if err != nil {
			_ = lockFile.Close()
		}
	}()
	if err = unix.Fstat(lockFD, &stat); err != nil {
		return nil, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0077 != 0 || int64(stat.Uid) != int64(os.Geteuid()) {
		return nil, failure("unsafe recovery lock file")
	}
	lock := unix.LOCK_EX
	if readonly {
		lock = unix.LOCK_SH
	}
	if err = unix.Flock(lockFD, lock|unix.LOCK_NB); err != nil {
		return nil, failure("recovery file already in use: %w", err)
	}
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	if readonly {
		q.Set("mode", "ro")
	} else {
		q.Set("mode", "rw")
	}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	if err = db.Ping(); err != nil {
		return nil, err
	}
	if !readonly {
		if _, err = db.Exec("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON"); err != nil {
			return nil, err
		}
	}
	if create {
		// Persist directory entry before any store mutation can refer to this journal.
		dir, e := os.Open(filepath.Dir(path))
		if e != nil {
			return nil, e
		}
		e = dir.Sync()
		closeErr := dir.Close()
		if e != nil {
			return nil, e
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return &lockedDB{db: db, file: lockFile}, nil
}

func (d *lockedDB) Close() error {
	return combineErrors(d.db.Close(), d.file.Close())
}

type journal struct{ *lockedDB }

func openJournal(path string, resume bool, digest, identity string) (_ *journal, err error) {
	db, err := openPrivateDB(path, !resume, false)
	if err != nil {
		return nil, err
	}
	j := &journal{db}
	defer func() {
		if err != nil {
			_ = j.Close()
		}
	}()
	if !resume {
		if _, err = j.db.Exec("CREATE TABLE state (key TEXT PRIMARY KEY, value BLOB NOT NULL)"); err != nil {
			return nil, err
		}
		header, _ := json.Marshal([]string{"replay-recovery-journal-v1", digest, identity})
		if err = j.put("header", header); err != nil {
			return nil, err
		}
	} else {
		b, e := j.get("header")
		if e != nil {
			return nil, e
		}
		var header []string
		if e = json.Unmarshal(b, &header); e != nil {
			return nil, e
		}
		if len(header) != 3 || header[0] != "replay-recovery-journal-v1" || header[1] != digest || header[2] != identity {
			return nil, failure("journal belongs to a different manifest, backend, or version")
		}
	}
	return j, nil
}

func (j *journal) get(key string) ([]byte, error) {
	var b []byte
	err := j.db.QueryRow("SELECT value FROM state WHERE key=?", key).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return b, err
}

func (j *journal) put(key string, b []byte) error {
	_, err := j.db.Exec("INSERT INTO state(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, b)
	return err
}

type mutation struct {
	Kind   string  `json:"kind"`
	Child  string  `json:"child"`
	Before Record  `json:"before"`
	After  *Record `json:"after"`
	Done   bool    `json:"done"`
}

func (j *journal) finish(id string, m mutation) error {
	m.Done = true
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	after, err := json.Marshal(m.After)
	if err != nil {
		return err
	}
	tx, err := j.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is harmless
	for key, value := range map[string][]byte{"step/" + id: b, fmt.Sprintf("latest/%x", m.Before.Key): after} {
		if _, err = tx.Exec("INSERT INTO state(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value); err != nil {
			return err
		}
	}
	return tx.Commit()
}
