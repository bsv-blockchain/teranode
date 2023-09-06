package extra

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/bip32"
	"github.com/libsv/go-bk/chaincfg"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
)

type KeySet struct {
	master        *bip32.ExtendedKey
	Path          string
	PrivateKey    *bec.PrivateKey
	PublicKey     *bec.PublicKey
	PublicKeyHash []byte
	Script        *bscript.Script
}

func (k *KeySet) Address(mainnet bool) string {
	addr, err := bscript.NewAddressFromPublicKey(k.PrivateKey.PubKey(), mainnet)
	if err != nil {
		panic(err)
	}

	return addr.AddressString
}

func New() (*KeySet, error) {
	var seed [64]byte
	_, err := rand.Read(seed[:])
	if err != nil {
		return nil, err
	}

	master, err := bip32.NewMaster(seed[:], &chaincfg.MainNet)
	if err != nil {
		return nil, err
	}

	return NewFromExtendedKey(master, "")
}

func NewFromExtendedKeyStr(extendedKeyStr string, derivationPath string) (*KeySet, error) {
	extendedKey, err := bip32.NewKeyFromString(extendedKeyStr)
	if err != nil {
		return nil, err
	}

	return NewFromExtendedKey(extendedKey, derivationPath)
}

func NewFromExtendedKey(extendedKey *bip32.ExtendedKey, derivationPath string) (*KeySet, error) {
	var err error

	master := extendedKey

	if derivationPath != "" {
		extendedKey, err = extendedKey.DeriveChildFromPath(derivationPath)
		if err != nil {
			return nil, err
		}
	}

	privateKey, err := extendedKey.ECPrivKey()
	if err != nil {
		return nil, err
	}

	publicKey := privateKey.PubKey()

	script, err := bscript.NewP2PKHFromPubKeyEC(publicKey)
	if err != nil {
		return nil, err
	}

	return &KeySet{
		master:        master,
		Path:          derivationPath,
		PrivateKey:    privateKey,
		PublicKey:     publicKey,
		PublicKeyHash: publicKey.SerialiseCompressed(),
		Script:        script,
	}, nil
}

func (k *KeySet) DeriveChildFromPath(derivationPath string) (*KeySet, error) {
	return NewFromExtendedKey(k.master, derivationPath)
}

type wocUtxo struct {
	Txid     string `json:"tx_hash"`
	Vout     uint32 `json:"tx_pos"`
	Height   uint32 `json:"height"`
	Satoshis uint64 `json:"value"`
}

func (k *KeySet) GetUTXOs(mainnet bool) ([]*bt.UTXO, error) {
	// Get UTXOs from WhatsOnChain
	net := "test"
	if mainnet {
		net = "main"
	}
	resp, err := http.Get(fmt.Sprintf("https://api.whatsonchain.com/v1/bsv/%s/address/%s/unspent", net, k.Address(mainnet)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.New("failed to get utxos")
	}

	var wocUnspent []*wocUtxo
	err = json.NewDecoder(resp.Body).Decode(&wocUnspent)
	if err != nil {
		return nil, err
	}

	unspent := make([]*bt.UTXO, len(wocUnspent))
	for i, utxo := range wocUnspent {
		txIDHash, err := chainhash.NewHashFromStr(utxo.Txid)
		if err != nil {
			return nil, err
		}

		unspent[i] = &bt.UTXO{
			TxIDHash:      txIDHash,
			Vout:          utxo.Vout,
			LockingScript: k.Script,
			Satoshis:      utxo.Satoshis,
		}
	}

	return unspent, nil
}
