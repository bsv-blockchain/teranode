package blockchain

import (
	"crypto/subtle"
	"net/http"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/labstack/echo/v4"
)

func (b *Blockchain) grpcAuthOptions() *util.AuthOptions {
	return &util.AuthOptions{
		APIKey:               b.settings.GRPCAdminAPIKey,
		RequireAuthByDefault: true,
		PublicMethods: map[string]bool{
			blockchain_api.BlockchainAPI_HealthGRPC_FullMethodName: true,
		},
	}
}

// requireAdminAPIKey guards HTTP calls that reach handlers without going through gRPC.
func (b *Blockchain) requireAdminAPIKey(next echo.HandlerFunc) echo.HandlerFunc {
	key := b.settings.GRPCAdminAPIKey
	return func(c echo.Context) error {
		keys := c.Request().Header.Values("x-api-key")
		if util.ValidateRequiredAdminAPIKey(key) != nil || len(keys) != 1 || subtle.ConstantTimeCompare([]byte(keys[0]), []byte(key)) != 1 {
			return c.NoContent(http.StatusUnauthorized)
		}
		return next(c)
	}
}
