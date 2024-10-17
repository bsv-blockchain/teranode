package p2p

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
)

type BanInfo struct {
	ExpirationTime time.Time
	Subnet         *net.IPNet
}

type BanList struct {
	logger      ulogger.Logger
	bannedPeers map[string]BanInfo
}

func NewBanList(logger ulogger.Logger) *BanList {
	return &BanList{
		logger:      logger,
		bannedPeers: make(map[string]BanInfo),
	}
}

// map of banned ips with their expiration time
// if the ip or subnet is already banned, the expiration time will be updated
// if the ip or subnet is not banned, it will be added to the map
// ipOrSubnet can be an IP address or a subnet in CIDR notation
func (b *BanList) Add(ipOrSubnet string, expirationTime time.Time) error {
	var subnet *net.IPNet

	var err error

	var key string

	if strings.Contains(ipOrSubnet, "/") {
		_, subnet, err = net.ParseCIDR(ipOrSubnet)
		if err != nil {
			b.logger.Errorf("error parsing ip or subnet: %v", err)

			return err
		}

		key = subnet.String()
	} else {
		ip := net.ParseIP(ipOrSubnet)
		if ip == nil {
			b.logger.Errorf("Can't parse IP or subnet: %s", ipOrSubnet)

			return err
		}

		if ip.To4() != nil {
			_, subnet, err = net.ParseCIDR(fmt.Sprintf("%s/32", ipOrSubnet))
		} else {
			_, subnet, err = net.ParseCIDR(fmt.Sprintf("%s/128", ipOrSubnet))
		}

		key = ipOrSubnet
	}

	if err != nil {
		return err
	}

	b.bannedPeers[key] = BanInfo{
		ExpirationTime: expirationTime,
		Subnet:         subnet,
	}

	return nil
}
func (b *BanList) Remove(ipOrSubnet string) error {
	if strings.Contains(ipOrSubnet, "/") {
		// It's a subnet
		_, subnet, err := net.ParseCIDR(ipOrSubnet)
		if err != nil {
			return errors.New(fmt.Sprintf("can't parse subnet: %s", ipOrSubnet))
		}

		delete(b.bannedPeers, subnet.String())
	} else {
		// It's an IP address
		ip := net.ParseIP(ipOrSubnet)
		if ip == nil {
			return errors.New(fmt.Sprintf("can't parse IP: %s", ipOrSubnet))
		}

		delete(b.bannedPeers, ipOrSubnet)
	}

	return nil
}

// IsBanned checks if a given IP address is banned
func (b *BanList) IsBanned(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		b.logger.Errorf("Invalid IP address: %s", ipStr)
		return false
	}

	now := time.Now()

	for key, banInfo := range b.bannedPeers {
		// First, check if the ban has expired
		if now.After(banInfo.ExpirationTime) {
			delete(b.bannedPeers, key)
			continue
		}

		// Check if it's a direct IP match
		if key == ipStr {
			return true
		}

		// Check if the IP is in the subnet
		if banInfo.Subnet.Contains(ip) {
			return true
		}
	}

	return false
}
