// Copyright 2018-present the CoreDHCP Authors. All rights reserved
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package redis

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/coredhcp/coredhcp/handler"
	"github.com/coredhcp/coredhcp/logger"
	"github.com/coredhcp/coredhcp/plugins"
	"github.com/gomodule/redigo/redis"
	"github.com/insomniacslk/dhcp/dhcpv4"
	"github.com/insomniacslk/dhcp/dhcpv6"
)

// Plugin wraps plugin registration information
var Plugin = plugins.Plugin{
	Name:   "redis",
	Setup6: setup6,
	Setup4: setup4,
}

// various global variables
var (
	log  = logger.GetLogger("plugins/redis")
	pool *redis.Pool
	TTL  int 
	TTLflag = false 
)

// Handler6 handles DHCPv6 packets for the redis plugin
func Handler6(req, resp dhcpv6.DHCPv6) (dhcpv6.DHCPv6, bool) {
	// TODO add IPv6 support
	m, err := req.GetInnerMessage()

	if err != nil {
		log.Errorf("BUG: could not decapsulate: %v", err)
		return resp, false 
	}

	if m.Options.OneIANA() == nil {
		log.Debug("No address requested")
		return resp, false
	}
	// 查看duid，duid对一台电脑是默认的。
	// duid :=m.Options.ClientID()
	// log.Printf("DUID: %v", duid)

	mac, err := dhcpv6.ExtractMAC(req)
    if err != nil {
		log.Warningf("Could not find client MAC, passing")
		return resp, false
	}
	log.Debugf("looking up an IPv6 address for MAC %s", mac.String())

    // 其他插件里可能会有var recLock sync.RWMutex  是因为全局变量
	// recLock.RLock()   // 这里是一个读锁  
	// defer recLock.RUnlock() // 但是由于我用的是redis读取，支持多个客户端读取，暂时并没有需要读锁的场景
    // 目前场景也比较单一，暂时不考虑加锁和多客户端的阻塞

	// Get redis connection from pool
	conn := pool.Get()
	defer conn.Close()
    
	options, err := redis.StringMap(conn.Do("HGETALL", "mac:"+mac.String()))
	
	if err != nil {
		log.Printf("Redis error: %s...dropping request", err)
		return resp, false
	}

	if len(options) == 0 {
		log.Printf("MAC %s not found...dropping request", mac.String())
		return resp, false
	}
    
	optIAA := &dhcpv6.OptIAAddress{}
    optIANA := &dhcpv6.OptIANA{}
	// Parse options and update response
	for option, value := range options {
		switch option {
		case "ipv6":
			ip := net.ParseIP(value)
			if ip == nil || ip.To16() == nil {
				log.Printf("MAC %s has invalid IPv6 address: %s...dropping request", mac.String(), value)
				return resp, false
			}

			optIAA.IPv6Addr = ip
			optIAA.PreferredLifetime = 3*time.Hour
			optIAA.ValidLifetime = 3*time.Hour 
		
        case "t1":
			T1, err := time.ParseDuration(value)
			if err != nil {
				log.Printf("MAC %s invalid T1 time %s...option skipped", mac.String(), value)
				break
			}
            if T1 > optIAA.ValidLifetime{
				optIANA.T1 = optIAA.ValidLifetime/2
			} else {
				optIANA.T1 = T1 
			}
			     
		case "t2":
			T2, err := time.ParseDuration(value)
			if err != nil {
				log.Printf("MAC %s invalid T2 time %s...option skipped", mac.String(), value)
				break
			}
			if T2 > optIAA.ValidLifetime{
				optIANA.T2 = optIAA.ValidLifetime * 4 / 5
			} else {
				optIANA.T2 = T2
			}
		}
	}

	optIANA.IaId = m.Options.OneIANA().IaId
	optIANA.Options = dhcpv6.IdentityOptions{Options: []dhcpv6.Option{optIAA}}
    log.Printf("OK, Assigned IPv6 %s to MAC %s by redis", optIAA.IPv6Addr, mac.String())

	resp.AddOption(optIANA)
	return resp, true
}

// Handler4 handles DHCPv4 packets for the redis plugin
func Handler4(req, resp *dhcpv4.DHCPv4) (*dhcpv4.DHCPv4, bool) {  // bool的意思是 true 结束处理输出响应  false继续处理 方法在server/handle.go
	
	// optionValue := resp.Options.Get(dhcpv4.OptionDomainNameServer) // 获取dns
	// if optionValue != nil {
	// 	log.Printf("DNS option: %v", optionValue)
	// } else {
	// 	log.Printf("DNS option not set")
	// }
	// Get redis connection from pool
	conn := pool.Get()

	// defer redis connection close so we don't leak connections
	defer conn.Close()

	// log.Printf()

	// Get all options for a MAC
	options, err := redis.StringMap(conn.Do("HGETALL", "mac:"+req.ClientHWAddr.String()))

	// Handle redis error
	if err != nil {
		log.Printf("Redis error: %s...dropping request", err)
		return resp, false
	}

	// Handle no hash found
	if len(options) == 0 {
		log.Printf("MAC %s not found...dropping request", req.ClientHWAddr.String())
		return resp, false
	}

	// Handle no ipv4 field
	if options["ipv4"] == "" {
		log.Printf("MAC %s has no ipv4 field...dropping request", req.ClientHWAddr.String())
		return resp, false
	}

	// Loop through options returned and assign as needed
	for option, value := range options {
		switch option {
		case "ipv4":
			ipaddr, ipnet, err := net.ParseCIDR(value)
			if err != nil {
				log.Printf("MAC %s malformed IP %s error: %s...dropping request", req.ClientHWAddr.String(), value, err)
				return resp, false
			}
			resp.YourIPAddr = ipaddr
			resp.Options.Update(dhcpv4.OptSubnetMask(ipnet.Mask))
			log.Printf("MAC %s assigned IPv4 address %s by redis", req.ClientHWAddr.String(), value)

		case "router":
			router := net.ParseIP(value)
			if router.To4() == nil {
				log.Printf("MAC %s Invalid router option: %s...option skipped", req.ClientHWAddr.String(), value)
				break
			}
			resp.Options.Update(dhcpv4.OptRouter(router))

		case "dns":
			var dnsServers4 []net.IP
			var flag = false
			servers := strings.Split(value, ",")
			for _, server := range servers {
				DNSServer := net.ParseIP(server)
				if DNSServer.To4() == nil {
					log.Printf("MAC %s Invalid dns in redis : %s...dropping request", req.ClientHWAddr.String(), server)
					// return resp, false
					flag = true
					break
				}
				dnsServers4 = append(dnsServers4, DNSServer)
			}
			if !flag && req.IsOptionRequested(dhcpv4.OptionDomainNameServer) {
				resp.Options.Update(dhcpv4.OptDNS(dnsServers4...))
			}

		case "leaseTime":
			lt, err := time.ParseDuration(value)
			if err != nil {
				log.Printf("MAC %s invalid lease time %s...option skipped", req.ClientHWAddr.String(), value)
				break
			}
			// Set lease time
			resp.Options.Update(dhcpv4.OptIPAddressLeaseTime(lt))

		// default:
			// log.Printf("MAC %s found un-handled option %s...option skipped", req.ClientHWAddr.String(), option)
		}
	}

	return resp, true
}

func setup6(args ...string) (handler.Handler6, error) {
	// TODO setup function for IPv6
	h6, _, err := setupRedis(true, args...) 
	return h6, err
}

func setup4(args ...string) (handler.Handler4, error) {
	_, h4, err := setupRedis(false, args...)
	return h4, err
}

var ShareConfig string

func setupRedis(v6 bool, args ...string) (handler.Handler6, handler.Handler4, error) {
	if len(args) < 1 {
		return nil, nil, fmt.Errorf("invalid number of arguments, want: 1 (redis server:port), got: %d", len(args))
	}
	if args[0] == "" {
		return nil, nil, errors.New("redis server can't be empty")
	}

	if v6 {    
		log.Printf("Using redis server %s for DHCPv6 static leases", args[0])
	} else {
		log.Printf("Using redis server %s for DHCPv4 static leases", args[0])
	}


    if len(args) > 2 {
		var err error  
		TTL, err = ParseToSeconds(args[2])
		if err != nil {
			log.Printf("invalid TTL, canceled redis update: %v", err)
			TTLflag = false 
		} else {
			TTLflag = true
		}
	}

	// Initialize Redis Pool     // 最大空闲连接10  超时时间240秒
	pool = &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", args[0], redis.DialPassword(args[1]))
		},
	}

	ShareConfig = args[0] + "," + args[1]

	return Handler6, Handler4, nil
}

// 解析时间参数并返回秒数
func ParseToSeconds(in string) (int, error) {
	duration, err := time.ParseDuration(in)
	if err != nil {
		return 0, err 
	}
	return int(duration.Seconds()), nil
}