package test_framework

import (
	"context"
	"time"

	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

type BitcoinTestFramework struct {
	ComposeFilePaths []string
	Context          context.Context
	Compose          tc.ComposeStack
	ctx              context.Context
}

func NewBitcoinTestFramework(composeFilePaths []string) *BitcoinTestFramework {
	return &BitcoinTestFramework{
		ComposeFilePaths: composeFilePaths,
		Context:          context.Background(),
	}
}

func (b *BitcoinTestFramework) SetupNodes() error {
	compose, err := tc.NewDockerComposeWith(tc.WithStackFiles(b.ComposeFilePaths...))
	if err != nil {
		return err
	}

	b.ctx = context.Background()
	if err := compose.Up(b.ctx); err != nil {
		return err
	}

	// Wait for the services to be ready
	time.Sleep(10 * time.Second)

	b.Compose = compose
	return nil
}

// StopNodes stops the Bitcoin nodes.
func (b *BitcoinTestFramework) StopNodes() error {
	if b.Compose != nil {
		// Stop the Docker Compose services
		if err := b.Compose.Down(b.ctx); err != nil {
			return err
		}
	}
	return nil
}
