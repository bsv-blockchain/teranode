package validator

import (
	"testing"
)

func TestNewDefaultOptions(t *testing.T) {
	opts := NewDefaultOptions()

	if opts.SkipUtxoCreation {
		t.Error("Default SkipUtxoCreation should be false")
	}

	if !opts.AddTXToBlockAssembly {
		t.Error("Default AddTXToBlockAssembly should be true")
	}

	if opts.SkipPolicyChecks {
		t.Error("Default SkipPolicyChecks should be false")
	}

	if opts.CreateConflicting {
		t.Error("Default CreateConflicting should be false")
	}
}

func TestProcessOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []Option
		expected Options
	}{
		{
			name: "No options",
			opts: []Option{},
			expected: Options{
				SkipUtxoCreation:     false,
				AddTXToBlockAssembly: true,
				SkipPolicyChecks:     false,
				CreateConflicting:    false,
			},
		},
		{
			name: "Single option",
			opts: []Option{
				WithSkipUtxoCreation(true),
			},
			expected: Options{
				SkipUtxoCreation:     true,
				AddTXToBlockAssembly: true,
				SkipPolicyChecks:     false,
				CreateConflicting:    false,
			},
		},
		{
			name: "Multiple options",
			opts: []Option{
				WithSkipUtxoCreation(true),
				WithAddTXToBlockAssembly(false),
				WithSkipPolicyChecks(true),
				WithCreateConflicting(true),
			},
			expected: Options{
				SkipUtxoCreation:     true,
				AddTXToBlockAssembly: false,
				SkipPolicyChecks:     true,
				CreateConflicting:    true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ProcessOptions(tt.opts...)
			if result.SkipUtxoCreation != tt.expected.SkipUtxoCreation {
				t.Errorf("SkipUtxoCreation = %v, want %v", result.SkipUtxoCreation, tt.expected.SkipUtxoCreation)
			}

			if result.AddTXToBlockAssembly != tt.expected.AddTXToBlockAssembly {
				t.Errorf("AddTXToBlockAssembly = %v, want %v", result.AddTXToBlockAssembly, tt.expected.AddTXToBlockAssembly)
			}

			if result.SkipPolicyChecks != tt.expected.SkipPolicyChecks {
				t.Errorf("SkipPolicyChecks = %v, want %v", result.SkipPolicyChecks, tt.expected.SkipPolicyChecks)
			}

			if result.CreateConflicting != tt.expected.CreateConflicting {
				t.Errorf("CreateConflicting = %v, want %v", result.CreateConflicting, tt.expected.CreateConflicting)
			}
		})
	}
}

func TestWithSkipUtxoCreation(t *testing.T) {
	tests := []struct {
		name     string
		skip     bool
		expected bool
	}{
		{"Skip UTXO creation", true, true},
		{"Don't skip UTXO creation", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ProcessOptions(WithSkipUtxoCreation(tt.skip))
			if opts.SkipUtxoCreation != tt.expected {
				t.Errorf("SkipUtxoCreation = %v, want %v", opts.SkipUtxoCreation, tt.expected)
			}
		})
	}
}

func TestWithAddTXToBlockAssembly(t *testing.T) {
	tests := []struct {
		name     string
		add      bool
		expected bool
	}{
		{"Add to block assembly", true, true},
		{"Don't add to block assembly", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ProcessOptions(WithAddTXToBlockAssembly(tt.add))
			if opts.AddTXToBlockAssembly != tt.expected {
				t.Errorf("AddTXToBlockAssembly = %v, want %v", opts.AddTXToBlockAssembly, tt.expected)
			}
		})
	}
}

func TestWithSkipPolicyChecks(t *testing.T) {
	tests := []struct {
		name     string
		skip     bool
		expected bool
	}{
		{"Skip policy checks", true, true},
		{"Don't skip policy checks", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ProcessOptions(WithSkipPolicyChecks(tt.skip))
			if opts.SkipPolicyChecks != tt.expected {
				t.Errorf("SkipPolicyChecks = %v, want %v", opts.SkipPolicyChecks, tt.expected)
			}
		})
	}
}

func TestWithCreateConflicting(t *testing.T) {
	tests := []struct {
		name     string
		create   bool
		expected bool
	}{
		{"Create conflicting", true, true},
		{"Don't create conflicting", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ProcessOptions(WithCreateConflicting(tt.create))
			if opts.CreateConflicting != tt.expected {
				t.Errorf("CreateConflicting = %v, want %v", opts.CreateConflicting, tt.expected)
			}
		})
	}
}
