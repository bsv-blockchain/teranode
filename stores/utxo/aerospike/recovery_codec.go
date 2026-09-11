package aerospike

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
)

// Tagged values avoid JSON's loss of integer precision, binary values, and
// non-string map keys. Unsupported particles fail closed before any mutation.
type recoveryValue struct {
	Type  string          `json:"type"`
	Value string          `json:"value,omitempty"`
	Bytes []byte          `json:"bytes,omitempty"`
	List  []recoveryValue `json:"list,omitempty"`
	Map   []recoveryPair  `json:"map,omitempty"`
}
type recoveryPair struct {
	Key   recoveryValue `json:"key"`
	Value recoveryValue `json:"value"`
}

func encodeRecoveryValue(v interface{}) (recoveryValue, error) {
	n := recoveryValue{}
	switch x := v.(type) {
	case nil:
		n.Type = "nil"
	case bool:
		n.Type = "bool"
		n.Value = strconv.FormatBool(x)
	case int:
		n.Type = "int"
		n.Value = strconv.Itoa(x)
	case int64:
		n.Type = "int64"
		n.Value = strconv.FormatInt(x, 10)
	case string:
		n.Type = "string"
		n.Value = x
	case as.GeoJSONValue:
		n.Type = "geojson"
		n.Value = string(x)
	case as.HLLValue:
		n.Type = "hll"
		n.Bytes = []byte(x)
	case []byte:
		n.Type = "bytes"
		n.Bytes = x
	case float64:
		n.Type = "float64"
		n.Value = strconv.FormatUint(math.Float64bits(x), 16)
	case []interface{}:
		n.Type = "list"
		for _, v := range x {
			item, err := encodeRecoveryValue(v)
			if err != nil {
				return n, err
			}
			n.List = append(n.List, item)
		}
	case map[interface{}]interface{}:
		n.Type = "map"
		for k, v := range x {
			key, err := encodeRecoveryValue(k)
			if err != nil {
				return n, err
			}
			value, err := encodeRecoveryValue(v)
			if err != nil {
				return n, err
			}
			n.Map = append(n.Map, recoveryPair{key, value})
		}
		sort.Slice(n.Map, func(i, j int) bool {
			a, _ := json.Marshal(n.Map[i].Key)
			b, _ := json.Marshal(n.Map[j].Key)
			return bytes.Compare(a, b) < 0
		})
	default:
		return n, errors.NewProcessingError("unsupported recovery particle %T", v)
	}
	return n, nil
}
func decodeRecoveryValue(n recoveryValue) (interface{}, error) {
	switch n.Type {
	case "nil":
		return nil, nil
	case "bool":
		return strconv.ParseBool(n.Value)
	case "int":
		return strconv.Atoi(n.Value)
	case "int64":
		return strconv.ParseInt(n.Value, 10, 64)
	case "string":
		return n.Value, nil
	case "geojson":
		return as.GeoJSONValue(n.Value), nil
	case "hll":
		return as.HLLValue(append([]byte{}, n.Bytes...)), nil
	case "bytes":
		return append([]byte{}, n.Bytes...), nil
	case "float64":
		v, err := strconv.ParseUint(n.Value, 16, 64)
		return math.Float64frombits(v), err
	case "list":
		v := make([]interface{}, len(n.List))
		for i, item := range n.List {
			var err error
			v[i], err = decodeRecoveryValue(item)
			if err != nil {
				return nil, err
			}
		}
		return v, nil
	case "map":
		v := make(map[interface{}]interface{}, len(n.Map))
		for _, item := range n.Map {
			k, err := decodeRecoveryValue(item.Key)
			if err != nil {
				return nil, err
			}
			if k != nil && !reflect.TypeOf(k).Comparable() {
				return nil, errors.NewProcessingError("non-comparable map key")
			}
			if _, ok := v[k]; ok {
				return nil, errors.NewProcessingError("duplicate map key")
			}
			value, err := decodeRecoveryValue(item.Value)
			if err != nil {
				return nil, err
			}
			v[k] = value
		}
		return v, nil
	default:
		return nil, errors.NewProcessingError("unsupported recovery type %q", n.Type)
	}
}
func encodeRecoveryBins(bins as.BinMap) ([]byte, error) {
	values := make(map[string]recoveryValue, len(bins))
	for key, value := range bins {
		n, err := encodeRecoveryValue(value)
		if err != nil {
			return nil, err
		}
		values[key] = n
	}
	return json.Marshal(values)
}
func decodeRecoveryBins(data []byte) (as.BinMap, error) {
	var values map[string]recoveryValue
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&values); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return nil, errors.NewProcessingError("trailing recovery data")
	}
	if values == nil {
		return nil, errors.NewProcessingError("missing recovery bins")
	}
	bins := make(as.BinMap, len(values))
	for key, n := range values {
		v, err := decodeRecoveryValue(n)
		if err != nil {
			return nil, err
		}
		bins[key] = v
	}
	return bins, nil
}
