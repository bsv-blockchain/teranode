package debugflags

import "testing"

func Test_EnabledHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		flags    Flags
		wantFile bool
		wantBlob bool
		wantUTXO bool
	}{
		{
			name:     "all disabled",
			flags:    Flags{},
			wantFile: false,
			wantBlob: false,
			wantUTXO: false,
		},
		{
			name:     "file enabled",
			flags:    Flags{File: true},
			wantFile: true,
			wantBlob: true, // blob logs piggyback on file flag
			wantUTXO: true, // utxo logs piggyback on file flag
		},
		{
			name:     "blob only",
			flags:    Flags{Blobstore: true},
			wantFile: false,
			wantBlob: true,
			wantUTXO: false,
		},
		{
			name:     "utxo only",
			flags:    Flags{UTXOStore: true},
			wantFile: false,
			wantBlob: false,
			wantUTXO: true,
		},
		{
			name:     "all true overrides",
			flags:    Flags{All: true},
			wantFile: true,
			wantBlob: true,
			wantUTXO: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			Init(tt.flags)
			t.Cleanup(func() { Init(Flags{}) })

			if got := FileEnabled(); got != tt.wantFile {
				t.Fatalf("FileEnabled() = %v, want %v", got, tt.wantFile)
			}

			if got := BlobstoreEnabled(); got != tt.wantBlob {
				t.Fatalf("BlobstoreEnabled() = %v, want %v", got, tt.wantBlob)
			}

			if got := UTXOStoreEnabled(); got != tt.wantUTXO {
				t.Fatalf("UTXOStoreEnabled() = %v, want %v", got, tt.wantUTXO)
			}
		})
	}
}
