package logs

import (
	"bufio"
	"io"
	"os"
	"time"

	"github.com/fsnotify/fsnotify"
)

// FileWatcher tails a log file and emits new entries
type FileWatcher struct {
	path       string
	file       *os.File
	offset     int64
	lineNumber int
	entriesCh  chan []LogEntry
	stopCh     chan struct{}
	watcher    *fsnotify.Watcher
}

// NewFileWatcher creates a new file watcher for the specified path
func NewFileWatcher(path string) (*FileWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &FileWatcher{
		path:      path,
		entriesCh: make(chan []LogEntry, 100),
		stopCh:    make(chan struct{}),
		watcher:   watcher,
	}, nil
}

// Start begins watching the file for changes
// If tailLines > 0, it will read the last n lines before starting to tail
func (fw *FileWatcher) Start(tailLines int) error {
	file, err := os.Open(fw.path)
	if err != nil {
		return err
	}
	fw.file = file

	// Get initial file info
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	// Read initial content (last n lines if specified)
	if tailLines > 0 {
		entries := fw.readLastLines(tailLines)
		if len(entries) > 0 {
			fw.entriesCh <- entries
		}
	}

	// Set offset to end of file for tailing
	fw.offset = info.Size()

	// Watch the file for changes
	if err := fw.watcher.Add(fw.path); err != nil {
		file.Close()
		return err
	}

	// Start the watch goroutine
	go fw.watch()

	return nil
}

// readLastLines reads the last n lines from the file
func (fw *FileWatcher) readLastLines(n int) []LogEntry {
	if fw.file == nil {
		return nil
	}

	// Seek to beginning
	if _, err := fw.file.Seek(0, io.SeekStart); err != nil {
		return nil
	}

	// Read all lines (not efficient for huge files, but simple)
	var lines []string
	scanner := bufio.NewScanner(fw.file)
	// Increase buffer size for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	// Take last n lines
	start := 0
	if len(lines) > n {
		start = len(lines) - n
	}

	entries := make([]LogEntry, 0, len(lines)-start)
	for i := start; i < len(lines); i++ {
		fw.lineNumber++
		entry := ParseLogLine(lines[i], fw.lineNumber)
		entries = append(entries, entry)
	}

	// Update offset to end of file
	fw.offset, _ = fw.file.Seek(0, io.SeekEnd)

	return entries
}

// watch monitors the file for changes
func (fw *FileWatcher) watch() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-fw.stopCh:
			return

		case event, ok := <-fw.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) {
				fw.readNewContent()
			}
			// Handle log rotation (file recreated)
			if event.Has(fsnotify.Create) || event.Has(fsnotify.Remove) {
				fw.handleRotation()
			}

		case <-fw.watcher.Errors:
			// Ignore errors, continue watching

		case <-ticker.C:
			// Periodic check for new content (fallback for systems where fsnotify might miss events)
			fw.readNewContent()
		}
	}
}

// readNewContent reads any new content added to the file
func (fw *FileWatcher) readNewContent() {
	if fw.file == nil {
		return
	}

	// Check current file size
	info, err := fw.file.Stat()
	if err != nil {
		return
	}

	// Handle truncation (log rotation)
	if info.Size() < fw.offset {
		fw.offset = 0
		_, _ = fw.file.Seek(0, io.SeekStart)
	}

	// No new content
	if info.Size() <= fw.offset {
		return
	}

	// Seek to last read position
	_, _ = fw.file.Seek(fw.offset, io.SeekStart)

	// Read new lines
	scanner := bufio.NewScanner(fw.file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var entries []LogEntry
	for scanner.Scan() {
		fw.lineNumber++
		entry := ParseLogLine(scanner.Text(), fw.lineNumber)
		entries = append(entries, entry)
	}

	// Update offset
	fw.offset, _ = fw.file.Seek(0, io.SeekCurrent)

	// Send entries if any
	if len(entries) > 0 {
		select {
		case fw.entriesCh <- entries:
		default:
			// Channel full, drop entries (shouldn't happen with buffered channel)
		}
	}
}

// handleRotation handles log file rotation
func (fw *FileWatcher) handleRotation() {
	if fw.file != nil {
		fw.file.Close()
	}

	// Try to reopen the file
	for i := 0; i < 10; i++ {
		file, err := os.Open(fw.path)
		if err == nil {
			fw.file = file
			fw.offset = 0
			fw.lineNumber = 0
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Entries returns the channel of new log entries
func (fw *FileWatcher) Entries() <-chan []LogEntry {
	return fw.entriesCh
}

// Stop stops watching the file
func (fw *FileWatcher) Stop() {
	close(fw.stopCh)
	if fw.watcher != nil {
		fw.watcher.Close()
	}
	if fw.file != nil {
		fw.file.Close()
	}
}
