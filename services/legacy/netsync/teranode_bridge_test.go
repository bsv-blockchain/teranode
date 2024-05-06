package netsync

import (
	"context"
	"testing"

	"github.com/ordishs/gocore"

	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type NullStore struct{}

func (ns *NullStore) SetBlockHeight(height uint32) error {
	return nil
}

func (ns *NullStore) GetBlockHeight() (uint32, error) {
	return 0, nil
}

func (ns *NullStore) Health(ctx context.Context) (int, string, error) {
	return 0, "Validator test Null Store", nil
}

func (ns *NullStore) DeleteSpends(deleteSpends bool) {
}

func (ns *NullStore) Get(ctx context.Context, spend *utxo.Spend) (*utxo.Response, error) {
	return nil, nil
}

func (ns *NullStore) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	return nil
}

func (ns *NullStore) StoreFromHashes(ctx context.Context, txID chainhash.Hash, utxoHashes []chainhash.Hash, lockTime uint32) error {
	return nil
}

func (ns *NullStore) Spend(ctx context.Context, spends []*utxo.Spend) error {
	return nil
}

func (ns *NullStore) UnSpend(ctx context.Context, spends []*utxo.Spend) error {
	return nil
}

func (ns *NullStore) Delete(ctx context.Context, tx *bt.Tx) error {
	return nil
}

func TestTXc99c49da4c38af669dea436d3e73780dfdb6c1ecf9958baa52960e8baee30e73(t *testing.T) {
	gocore.Config().Set("blockassembly_disabled", "true")

	tx, err := bt.NewTxFromString("010000000000000000ef010276b76b07f4935c70acf54fbf1f438a4c397a9fb7e633873c4dd3bc062b6b40000000008c493046022100d23459d03ed7e9511a47d13292d3430a04627de6235b6e51a40f9cd386f2abe3022100e7d25b080f0bb8d8d5f878bba7d54ad2fda650ea8d158a33ee3cbd11768191fd004104b0e2c879e4daf7b9ab68350228c159766676a14f5815084ba166432aab46198d4cca98fa3e9981d0a90b2effc514b76279476550ba3663fdcaff94c38420e9d500000000404b4c00000000001976a914dc44b1164188067c3a32d4780f5996fa14a4f2d988ac0100093d00000000001976a9149a7b0f3b80c6baaeedce0a0842553800f832ba1f88ac00000000")
	require.NoError(t, err)

	ns := &NullStore{}

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, ns, memory.New(ulogger.TestLogger{}))
	require.NoError(t, err)

	ctx := context.Background()

	err = v.Validate(ctx, tx, 110300)
	require.NoError(t, err)
}

func TestTXfb0a1d8d34fa5537e461ac384bac761125e1bfa7fec286fa72511240fa66864d(t *testing.T) {
	gocore.Config().Set("blockassembly_disabled", "true")

	tx, err := bt.NewTxFromString("010000000000000000ef012316aac445c13ff31af5f3d1e2cebcada83e54ba10d15e01f49ec28bddc285aa000000008e4b3048022200002b83d59c1d23c08efd82ee0662fec23309c3adbcbd1f0b8695378db4b14e736602220000334a96676e58b1bb01784cb7c556dd8ce1c220171904da22e18fe1e7d1510db5014104d0fe07ff74c9ef5b00fed1104fad43ecf72dbab9e60733e4f56eacf24b20cf3b8cd945bcabcc73ba0158bf9ce769d43e94bd58c5c7e331a188922b3fe9ca1f5affffffffc0c62d00000000001976a9147a2a3b481ca80c4ba7939c54d9278e50189d94f988ac01c0c62d00000000001976a9147a2a3b481ca80c4ba7939c54d9278e50189d94f988ac00000000")
	require.NoError(t, err)

	assert.Equal(t, "fb0a1d8d34fa5537e461ac384bac761125e1bfa7fec286fa72511240fa66864d", tx.TxIDChainHash().String())

	ns := &NullStore{}

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, ns, memory.New(ulogger.TestLogger{}))
	require.NoError(t, err)

	ctx := context.Background()

	err = v.Validate(ctx, tx, 124276)
	require.NoError(t, err)
}
func TestTXd5a13dcb1ad24dbffab91c3c2ffe7aea38d5e84b444c0014eb6c7c31fe8e23fc(t *testing.T) {
	gocore.Config().Set("blockassembly_disabled", "true")

	tx, err := bt.NewTxFromString("010000000000000000ef06799f85b39bf61dac7dc36bf6c308ae59939418bc130ed759da11294f5ef9aaa6010000008c4930460221009c76c09e958f8e931e4554c67b058f97d69aebe388fe94d10304759d8e479b6f022100fd7170a3433163e6cbd26d72c5a1ef03208fd2f161eec8ed29a3e7375c110e850141048d05eb9a3e0460c9bc53de3d65338759847127478bb7b78f251f35296f6e7e81eb47938e36b13c208bfc0e5db5159283aa38b6bed9471f078358d278add30d6effffffff40420f00000000001976a914dc135692cc0f449107e16210370344326aeb779088acf74638454462945a25bf31fe6d86af21a9c4f759202d3e5a77af7159e9366d07000000008b4830450221009dce295a412b5e59346f8324489f840d8a4cf091cd937a404ede5741a88b5daa022002141dcef9e0292d633dd6e15887011f95fd93093cdbd6095fe4109ccf8fb3000141048d05eb9a3e0460c9bc53de3d65338759847127478bb7b78f251f35296f6e7e81eb47938e36b13c208bfc0e5db5159283aa38b6bed9471f078358d278add30d6effffffff40420f00000000001976a914dc135692cc0f449107e16210370344326aeb779088ac0d4c9e1044f3b45d1ac0774a611a0671230cfcb7f35a74312292b21e801842be000000008c493046022100c0a288dafa462de48d98dc57ff2122c83438c1a23dd3b02c396edef07fde24fd022100cbd739de346cece8fd7afa045ee734f6c3bd663ac4b8ab22a039e41306bbb2680141048d05eb9a3e0460c9bc53de3d65338759847127478bb7b78f251f35296f6e7e81eb47938e36b13c208bfc0e5db5159283aa38b6bed9471f078358d278add30d6effffffff00000000000000001976a914dc135692cc0f449107e16210370344326aeb779088ac5873172734313950c64ee7835a66078ea498dffe9a369b8d178c7afcadca1473000000008c493046022100dff57c5672ee29a561be549399a1f7d991d946dac90f0e17271ff1182cdf1cf2022100ac902f98a75e5cd4e7aa9e3f53902c4ae1abbde4bb61f37f8e9b9d7711c0560c0141048d05eb9a3e0460c9bc53de3d65338759847127478bb7b78f251f35296f6e7e81eb47938e36b13c208bfc0e5db5159283aa38b6bed9471f078358d278add30d6effffffff00000000000000001976a914dc135692cc0f449107e16210370344326aeb779088ac50098179a9cbaa23019a50e4fb13b3f7da468c4656d5b389b2eaa3abce402fdb000000008c493046022100fe83fe49c10b714f0686e08af05ebdedf7906a61e1747141929a999805edf5ba022100a9cb162bfdf4ad9a821443a900e4bddc75acfcaa42a924d43ce7ac672d7f89340141048d05eb9a3e0460c9bc53de3d65338759847127478bb7b78f251f35296f6e7e81eb47938e36b13c208bfc0e5db5159283aa38b6bed9471f078358d278add30d6effffffff00000000000000001976a914dc135692cc0f449107e16210370344326aeb779088ac91adfcbbca99c0dbf1efbae93d5fed774c420beac17468cbd4d484428b82793b000000008b483045022066b9120a711fa9b75da495b3c50ee5057f4fe476ed5955e8e69aed998429d2f2022100b53715ce6c320c5dfab5eff4bd9d9660159650a9519655ce143f5737fa87562a0141048d05eb9a3e0460c9bc53de3d65338759847127478bb7b78f251f35296f6e7e81eb47938e36b13c208bfc0e5db5159283aa38b6bed9471f078358d278add30d6effffffff40420f00000000001976a914dc135692cc0f449107e16210370344326aeb779088ac0146f41000000000001976a9146194bf3632a3cfca62f397aa94cd30de456e5ab388ac00000000")
	require.NoError(t, err)

	assert.Equal(t, "d5a13dcb1ad24dbffab91c3c2ffe7aea38d5e84b444c0014eb6c7c31fe8e23fc", tx.TxIDChainHash().String())

	ns := &NullStore{}

	assert.False(t, tx.IsExtended())

	assert.True(t, util.IsExtended(tx, 125777))

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, ns, memory.New(ulogger.TestLogger{}))
	require.NoError(t, err)

	ctx := context.Background()

	err = v.Validate(ctx, tx, 125777)
	require.NoError(t, err)
}

func TestTXeb3b82c0884e3efa6d8b0be55b4915eb20be124c9766245bcc7f34fdac32bccb(t *testing.T) {
	gocore.Config().Set("blockassembly_disabled", "true")

	tx, err := bt.NewTxFromString("010000000000000000ef024de8b0c4c2582db95fa6b3567a989b664484c7ad6672c85a3da413773e63fdb8000000006b48304502205b282fbc9b064f3bc823a23edcc0048cbb174754e7aa742e3c9f483ebe02911c022100e4b0b3a117d36cab5a67404dddbf43db7bea3c1530e0fe128ebc15621bd69a3b0121035aa98d5f77cd9a2d88710e6fc66212aff820026f0dad8f32d1f7ce87457dde50ffffffff30c11d00000000001976a914709dcb44da534c550dacf4296f75cba1ba3b317788ac4de8b0c4c2582db95fa6b3567a989b664484c7ad6672c85a3da413773e63fdb8010000006f004730440220276d6dad3defa37b5f81add3992d510d2f44a317fd85e04f93a1e2daea64660202200f862a0da684249322ceb8ed842fb8c859c0cb94c81e1c5308b4868157a428ee01ab51210232abdc893e7f0631364d7fd01cb33d24da45329a00357b3a7886211ab414d55a51aeffffffffc0c62d000000000017142a9bc5447d664c1d0141392a842d23dba45c4f13b17502e0fd1c00000000001976a914380cb3c594de4e7e9b8e18db182987bebb5a4f7088acc0c62d000000000017142a9bc5447d664c1d0141392a842d23dba45c4f13b17500000000")
	require.NoError(t, err)

	assert.Equal(t, "eb3b82c0884e3efa6d8b0be55b4915eb20be124c9766245bcc7f34fdac32bccb", tx.TxIDChainHash().String())

	ns := &NullStore{}

	assert.True(t, tx.IsExtended())

	assert.True(t, util.IsExtended(tx, 163685))

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, ns, memory.New(ulogger.TestLogger{}))
	require.NoError(t, err)

	ctx := context.Background()

	err = v.Validate(ctx, tx, 163685)
	require.NoError(t, err)
}
