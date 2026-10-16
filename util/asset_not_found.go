package util

// AssetNotFoundHeader marks an asset service 404 as a handler's own verdict that
// the resource it was asked for does not exist. Its value names the resource kind
// ("tx").
//
// A 404 without it can mean the route itself was not matched - a misconfigured
// asset URL answers every request that way - so a caller that turns a 404 into
// "no such object" checks for this header first. getrawtransaction does: it
// answers -5 only for a marked 404, and treats an unmarked one as the node fault
// it is.
const AssetNotFoundHeader = "X-Teranode-Not-Found"

// AssetNotFoundTx is AssetNotFoundHeader's value for a transaction.
const AssetNotFoundTx = "tx"
