package aerospike2

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/assert"

	aero "github.com/aerospike/aerospike-client-go/v7"
	aeroTest "github.com/ajeetdsouza/testcontainers-aerospike-go"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

func TestAerospikeContainer(t *testing.T) {
	aeroClient := setupAerospike(t)
	// your code here

	assert.NotNil(t, aeroClient)
	fmt.Printf("Aerospike client: %v\n", aeroClient)
}

func setupAerospike(t *testing.T) *aero.Client {
	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithImage("aerospike:ce-6.4.0.7_2"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err := container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := aero.NewClient(host, port)
	require.NoError(t, err)

	return client
}

func TestPreviousOutput(t *testing.T) {
	t.Skip("Skipping test that requires Aerospike")
	// Aerospike
	aeroURL, err := url.Parse("aerospike://localhost:3000/test")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, aeroURL)
	require.NoError(t, err)

	// 75696d1ef2b70f62c0157e7dd9cfef13f4f3ee5fae98cd04e00b1822d26363fa
	previousTx, err := bt.NewTxFromString("010000000000000000ef012d3746b3fa849a901dac83f4ee0bc9f8a0b294e69ee2a216fd0614c49f5aac9b000000008b483045022100b059ea8f390ab16cf2744203a12a4d0a6a85abf62aee68c09164551fc5a9c72e0220324cf2639078a8052b9a533cdd7a53b93cc32b4ecaa5c3326abf651107deef1d01410446ca32230229946b745df0b2a220084f8afd45ca0b40314999c73a84e8bdfcaaa31f60f756e4b4971492de4d4a18e820ad861f9a949ca3bb700ca1f24ddb9110ffffffffb0662ef1000000001976a914cb775ace861ff5e18fb5bfcc848ea7bbb3b347e288ac0230e20ff1000000001976a914ff9baa5b596c5aeedd9c2c41ade6883ffa88bc1f88ac80841e00000000001976a9149131dafa70cc0ac860c08873508c1aa3a72b31a988ac00000000")
	require.NoError(t, err)
	assert.Equal(t, "75696d1ef2b70f62c0157e7dd9cfef13f4f3ee5fae98cd04e00b1822d26363fa", previousTx.TxID())

	// 89c82039452c14a9314b5834e5d2b9241b1fdccdb6e4f4f68e49015540faaf95
	tx, err := bt.NewTxFromString("010000000000000000ef01fa6363d222180be004cd98ae5feef3f413efcfd97d7e15c0620fb7f21e6d6975000000008b483045022100c31fe8e263ae282633d36c761b65ff4a566f09ddf0da201f903a1e6c159b0ac4022074b80000b02ee8578db7522c7d7f7d2044dfa105f56e8fc1e9d27c97a033abd6014104beec61e3acf22e5aa6c97ae354fc65555e26dd444f620d113d7feba7a8815ab15205ee8fec4e13af1777610a91f6b030b072be663db972fe0c394411f0fb2bc1ffffffff30e20ff1000000001976a914ff9baa5b596c5aeedd9c2c41ade6883ffa88bc1f88ac0230d9d2f0000000001976a91470bcb5186044c09e532ddc2914b61c0a2b07876188ac00093d00000000001976a9149a58e9696fe10b44bf2c91b2cb18c2ded593041788ac00000000")
	require.NoError(t, err)
	assert.Equal(t, "89c82039452c14a9314b5834e5d2b9241b1fdccdb6e4f4f68e49015540faaf95", tx.TxID())

	ctx := context.Background()

	err = store.Delete(ctx, previousTx.TxIDChainHash())
	require.NoError(t, err)

	err = store.Delete(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	_, err = store.Create(ctx, previousTx)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx)
	require.NoError(t, err)

	// Now we can test the previous output

	outpoints := []*meta.PreviousOutput{
		{
			PreviousTxID: *previousTx.TxIDChainHash(),
			Vout:         0,
		},
		{
			PreviousTxID: *tx.TxIDChainHash(),
			Vout:         0,
		},
	}

	// Get the previous output
	err = store.PreviousOutputsDecorate(ctx, outpoints)
	require.NoError(t, err)

	// Check the outpoints
	assert.Len(t, outpoints, 2)
	assert.Equal(t, outpoints[0].Satoshis, previousTx.Outputs[0].Satoshis)
	assert.True(t, previousTx.Outputs[0].LockingScript.EqualsBytes(outpoints[0].LockingScript))
	assert.Equal(t, outpoints[1].Satoshis, tx.Outputs[0].Satoshis)
	assert.True(t, tx.Outputs[0].LockingScript.EqualsBytes(outpoints[1].LockingScript))
}

func TestCreateWith2Outputs(t *testing.T) {
	t.Skip("Skipping test that requires Aerospike")

	aeroURL, err := url.Parse("aerospike://localhost:3000/test")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, aeroURL)
	require.NoError(t, err)

	tx, err := bt.NewTxFromString("010000000000000000ef01c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff00f2052a0100000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000")
	require.NoError(t, err)

	ctx := context.Background()

	_, err = store.Create(ctx, tx)
	require.NoError(t, err)

}

func TestIsLargeTransaction(t *testing.T) {
	f, err := os.Open("bb41a757f405890fb0f5856228e23b715702d714d59bf2b1feb70d8b2b4e3e08.bin")
	require.NoError(t, err)

	defer f.Close()

	var tx bt.Tx

	_, err = tx.ReadFrom(f)
	require.NoError(t, err)

	assert.Equal(t, "bb41a757f405890fb0f5856228e23b715702d714d59bf2b1feb70d8b2b4e3e08", tx.TxIDChainHash().String())
	assert.True(t, tx.IsExtended())

	assert.Equal(t, 999657, len(tx.Bytes()))
	assert.Equal(t, 1189009, len(tx.ExtendedBytes()))

	assert.True(t, isLargeTransaction(len(tx.Bytes()), len(tx.Outputs)))
	assert.True(t, isLargeTransaction(len(tx.ExtendedBytes()), len(tx.Outputs)))
}
