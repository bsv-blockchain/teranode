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

func (b *BitcoinTestFramework) SetupNodes(m map[string]string) error {
	compose, err := tc.NewDockerCompose(b.ComposeFilePaths...)
	if err != nil {
		return err
	}

	b.ctx = context.Background()
	if err := compose.WithEnv(m).Up(b.ctx); err != nil {
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

func (b *BitcoinTestFramework) StopNode(nodeName string) error {
	if b.Compose != nil {
		// Stop the Docker Compose services
		node, err := b.Compose.ServiceContainer(b.ctx, nodeName)
		if err != nil {
			return err
		}

		err = node.Stop(b.ctx, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *BitcoinTestFramework) StartNode(nodeName string) error {
	if b.Compose != nil {
		// Stop the Docker Compose services
		node, err := b.Compose.ServiceContainer(b.ctx, nodeName)
		if err != nil {
			return err
		}

		err = node.Start(b.ctx)
		if err != nil {
			return err
		}

	}
	return nil
}
