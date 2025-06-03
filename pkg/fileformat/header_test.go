package fileformat

import (
	"bytes"
	"testing"
)

func TestHeaderWriteRead_RoundTrip(t *testing.T) {
	types := []FileType{FileTypeUtxoAdditions, FileTypeBlock, FileTypeTx, FileTypeSubtreeMeta}

	for _, ft := range types {
		h := NewHeader(ft)

		buf := &bytes.Buffer{}

		if err := h.Write(buf); err != nil {
			t.Fatalf("Header.Write failed: %v", err)
		}

		var h2 Header
		if err := h2.Read(buf); err != nil {
			t.Fatalf("Header.Read failed: %v", err)
		}

		if h.magic != h2.magic {
			t.Errorf("magic mismatch: got %q, want %q", h2.magic, h.magic)
		}
	}
}

func TestHeader_ReadHeaderFunc(t *testing.T) {
	ft := FileTypeTx

	h := NewHeader(ft)

	buf := &bytes.Buffer{}

	if err := h.Write(buf); err != nil {
		t.Fatalf("Header.Write failed: %v", err)
	}

	h2, err := ReadHeader(buf)
	if err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
	}

	if h.magic != h2.magic {
		t.Errorf("Header mismatch after ReadHeader: got %+v, want %+v", h2, h)
	}
}

func TestHeader_InvalidMagic(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.Write([]byte("INVALID\x00\x00")) // 8 bytes, not a known magic
	buf.Write(make([]byte, 36))          // pad for hash and hNumber

	var h Header

	if err := h.Read(buf); err == nil {
		t.Error("expected error for unknown magic, got nil")
	}
}

func TestHeader_ShortRead(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.Write([]byte("U-A-1.0")) // only 7 bytes, too short

	var h Header

	if err := h.Read(buf); err == nil {
		t.Error("expected error for short read, got nil")
	}
}
