package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/coredhcp/coredhcp/handler"
	"github.com/coredhcp/coredhcp/logger"
	"github.com/coredhcp/coredhcp/plugins"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/insomniacslk/dhcp/dhcpv4"
	"github.com/insomniacslk/dhcp/dhcpv6"
	predis "github.com/coredhcp/coredhcp/plugins/redis"
	"github.com/gomodule/redigo/redis"
)

var (
	log      = logger.GetLogger("plugins/postgres")
	pool       *pgxpool.Pool
	rpool      *redis.Pool 
	recLock    sync.RWMutex
	dbConfig = ""
	updateredis  = false
)

// Plugin wraps plugin registration information
var Plugin = plugins.Plugin{
	Name:   "postgres",
	Setup6: setup6,
	Setup4: setup4,
}

type IPDetails struct {
	mac_address string 
	Cidr      string 
	IPv6       net.IP
	IPv4       net.IP
	IPnet      *net.IPNet
	Router     net.IP
	DNS        []net.IP
	LeaseTime  time.Duration
	T1         time.Duration
	T2         time.Duration
}


// Handler6 handles DHCPv6 packets for the PostgreSQL plugin
func Handler6(req, resp dhcpv6.DHCPv6) (dhcpv6.DHCPv6, bool) {    // 因为pg插件目前默认是最后一个插件，所以true/false结果都是一样的跳出循环
	m, err := req.GetInnerMessage()
	if err != nil {
		log.Errorf("BUG: could not decapsulate request: %v", err)
		return nil, true
	}

	if m.Options.OneIANA() == nil {
		log.Debug("No address requested in the DHCPv6 message")
		return resp, false
	}

	mac, err := dhcpv6.ExtractMAC(req)
	if err != nil {
		log.Warningf("Failed to extract MAC address: %v", err)
		return resp, false
	}
	log.Debugf("Looking up IPv6 address for MAC %s", mac.String())

	recLock.RLock()
	details, err := queryFromDB(mac.String(), 6)
	recLock.RUnlock()
	if err != nil {
		log.Warningf("MAC %s error: %v", mac.String(), err)
		return resp, true 
	}
    
	optIAA := &dhcpv6.OptIAAddress{}
	optIANA := &dhcpv6.OptIANA{}

	optIAA.IPv6Addr = details.IPv6
	optIAA.PreferredLifetime = 3 * time.Hour
	optIAA.ValidLifetime = 3 * time.Hour
    
	if details.T1 >= optIAA.ValidLifetime{
		optIANA.T1 = optIAA.ValidLifetime / 2
	} else if details.T1 != 0 {
        optIANA.T1 = details.T1
	}

	if details.T2 >= optIAA.ValidLifetime{
		optIANA.T2 = optIAA.ValidLifetime * 4 / 5
	} else if details.T2 != 0 {
        optIANA.T2 = details.T2
	}

    optIANA.IaId = m.Options.OneIANA().IaId
	optIANA.Options = dhcpv6.IdentityOptions{Options: []dhcpv6.Option{optIAA}}
	log.Printf("OK, Assigned IPv6 %s to MAC %s by pg", details.IPv6, mac.String())
    resp.AddOption(optIANA)
    
	if updateredis {
		err := redisUpdate(details, 6)
		if err != nil {
			fmt.Printf("update redis err:%v", err)
		}
	}

	return resp, false
}

// Handler4 handles DHCPv4 packets for the PostgreSQL plugin
func Handler4(req, resp *dhcpv4.DHCPv4) (*dhcpv4.DHCPv4, bool) {
	recLock.RLock()
	details, err := queryFromDB(req.ClientHWAddr.String(), 4)
	recLock.RUnlock()
	if err != nil {
		log.Warningf("MAC %s error: %v", req.ClientHWAddr.String(), err)
		return nil, true
	}

	resp.YourIPAddr = details.IPv4 
	resp.Options.Update(dhcpv4.OptSubnetMask(details.IPnet.Mask))
	if details.Router != nil {
		resp.Options.Update(dhcpv4.OptRouter(details.Router))
	}
	if len(details.DNS) > 0 && req.IsOptionRequested(dhcpv4.OptionDomainNameServer){
		resp.Options.Update(dhcpv4.OptDNS(details.DNS...))
	}

	if details.LeaseTime != 0 {
		resp.Options.Update(dhcpv4.OptIPAddressLeaseTime(details.LeaseTime))
	}
	
	log.Printf("Assigned IPv4 address %s with details to MAC %s by pg", details.IPv4, req.ClientHWAddr.String())
	
	if updateredis {
       err := redisUpdate(details, 4)
	   if err != nil {
		  log.Printf("update redis err:%v", err)
	   }
	}
	return resp, true
}

func setup6(args ...string) (handler.Handler6, error) {
	// log.Debugf("Setting up Handler6 with arguments: %v", args)
	h6, _, err := setupDB(true, args...)
	if err != nil {
		log.Errorf("Failed to set up Handler6: %v", err)
	}
	return h6, err
}

func setup4(args ...string) (handler.Handler4, error) {
	// log.Debugf("Setting up Handler4 with arguments: %v", args)
	_, h4, err := setupDB(false, args...)
	if err != nil {
		log.Errorf("Failed to set up Handler4: %v", err)
	}
	return h4, err
}

func setupDB(v6 bool, args ...string) (handler.Handler6, handler.Handler4, error) {
	if len(args) < 1 {
		return nil, nil, errors.New("postgreSQL connection url string required")
	}

	if args[0] == ""{
		return nil, nil, errors.New("postgreSQL url can't be empty")
	}
	log.Debugf("Initializing PostgreSQL database with connection string: %s", args[0])
	
	if v6 {    
		log.Printf("Using pg %s for DHCPv6 static leases", args[0])
	} else {
		log.Printf("Using pg %s for DHCPv4 static leases", args[0])
	}


	dbConfig = args[0]
    
	if len(args) > 1 && args[1] != ""{
		if args[1] == "true" || args[1] == "1" || args[1] == "True" || args[1] == "TRUE" {
			updateredis = true
		} else {
			updateredis = false
		}
	}
	if updateredis { // 目前pg 插件第二个参数控制redis的连接与更新
		redisloc, redispassword := getRedisConfig()
		rpool = &redis.Pool{
			MaxIdle: 10,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", redisloc, redis.DialPassword(redispassword))
			},
		}
	}

    // 连接pg
	config, err := pgxpool.ParseConfig(dbConfig)

	if err != nil {
		log.Printf("Failed to open PostgreSQL config: %v", err)
		return nil, nil, fmt.Errorf("%v", err)
	}

	config.MaxConns = 10
	// config.MinConns = 2
	config.MaxConnIdleTime = 240 * time.Second
    
	pool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Printf("Failed to open PostgreSQL connection: %v", err)
		return nil, nil, fmt.Errorf("%v", err)
	}

	log.Infof("Connected to PostgreSQL successfully")

	return Handler6, Handler4, nil
}

func queryFromDB(mac string, version int) (*IPDetails, error) {
    conn, err := pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	log.Debugf("Querying details for MAC %s and IPv%d", mac, version)
    
	var answer = &IPDetails{}
	if version == 6 {
        var macAddr, ipv6, t1, t2 string 

		query := `SELECT mac_address, ipv6, t1, t2 FROM coredhcp_records WHERE mac_address = $1`
        
		// row, err := conn.Query(context.Background(), query, "mac"+mac)
		//  values, err := row.Values() // values可以用for range枚举
		
		err := conn.QueryRow(context.Background(), query, "mac:" + mac).Scan(&macAddr, &ipv6, &t1, &t2)
		answer.mac_address = macAddr
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				log.Warningf("No IPv%v record found for MAC %s", version, mac)
				return nil, fmt.Errorf("no record found for MAC %s", mac)
			}
			log.Errorf("Database query error for MAC %s: %v", mac, err)
		    return nil, fmt.Errorf("query error: %w", err)
		}

		answer.IPv6 = net.ParseIP(ipv6)
		if answer.IPv6 == nil || answer.IPv6.To16() == nil {
			log.Printf("MAC %s has invalid IPv6 address: %s", mac, ipv6)
			return nil, fmt.Errorf("invalid IPv6 address")
		}

		T1, err := time.ParseDuration(t1)
		if err != nil {
			log.Printf("MAC %s has invalid T1 time %s", mac, t1)
		} else {
			answer.T1 = T1
		}

		T2, err := time.ParseDuration(t2)
		if err != nil {
			log.Printf("MAC %s has invalid T2 time %s", mac, t2)
		} else {
			answer.T2 = T2
		}

		return answer, nil 
	} else if version == 4{
		var macAddr, ipv4, router, dns, leasetime string

		query := `SELECT mac_address, ipv4, router, dns, lease_time FROM coredhcp_records WHERE mac_address = $1`
        err := conn.QueryRow(context.Background(), query, "mac:" + mac).Scan(&macAddr, &ipv4, &router, &dns, &leasetime)
	    answer.mac_address = macAddr
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				log.Warningf("No IPv%v record found for MAC %s", version, mac)
				return nil, fmt.Errorf("no record found for MAC %s", mac)
			}
			log.Errorf("Database query error for MAC %s: %v", mac, err)
		    return nil, fmt.Errorf("query error: %w", err)
		}

		ipaddr, ipnet, err := net.ParseCIDR(ipv4)
        if err != nil {
			log.Printf("MAC %s has invalid IPv4 address: %s", mac, ipv4)
			return nil, fmt.Errorf("invalid IPv4 address")
		}
		answer.IPv4 = ipaddr
		answer.Cidr = ipv4
		answer.IPnet = ipnet
		// log.Printf("ipv4 %v, %v, %v ", answer.IPv4, answer.Cidr, answer.IPnet)

		R := net.ParseIP(router)
		if R.To4() == nil {
			log.Printf("MAC %s has invalid router option: %s", mac, router)
		} else {
			answer.Router = R
		}

		servers := strings.Split(dns, ",")
        for _, server := range servers {
			DNSServer :=  net.ParseIP(server)
			if DNSServer.To4() == nil {
				log.Printf("MAC %s in pg has invalid dns server: %s", mac, server)
				break;
			}
			answer.DNS = append(answer.DNS, DNSServer)
		}

		lt, err := time.ParseDuration(leasetime)
		if err != nil {
			log.Printf("MAC %s has invalid lease time: %s", mac, leasetime)
			answer.LeaseTime = 0
		} else {
			answer.LeaseTime = lt
		}

		return answer, nil
	}
    return nil, fmt.Errorf("invalid version")
}

func getRedisConfig()(string, string) {
	var args = make([]string, 2)
	configs := strings.Split(predis.ShareConfig, ",")
	
	for index, config := range configs {
		if index == 0 {
			args[index] = config
		} else if index == 1 && config != "" {
            args[index] = config
		}
	}
	return args[0], args[1]
}

func redisUpdate(d *IPDetails, v int)(error){
	conn := rpool.Get()
	defer conn.Close()
    
	if v == 6 {
		mapp := map[string]string{
			"ipv6":       d.IPv6.String(),
			"t1":         d.T1.String(),
			"t2":         d.T2.String(),
		}
        
		for field, value := range mapp {
			_, err := conn.Do("HSET", d.mac_address, field, value)
			if err != nil {
				log.Printf("Update redis error: HSET %v %v %v", d.mac_address, field, value)
				return fmt.Errorf("%v", err)
			}
		}
		log.Printf("update redis succeed v6")
        
	} else  if v == 4 {
		mapp := map[string]string{
			"ipv4":       d.Cidr,
			"router":     d.Router.String(),
			"dns":        d.DNSAsString(),
			"leaseTime":  d.LeaseTime.String(),
		}

		for field, value := range mapp {
			_, err := conn.Do("HSET", d.mac_address, field, value)
			if err != nil {
				log.Printf("Update redis error: HSET %v %v %v", d.mac_address, field, value)
				return fmt.Errorf("%v", err)
			}
		}
		log.Printf("update redis succeed v4")
	}
    
	if predis.TTLflag && predis.TTL > 0 {
		_, err := conn.Do("EXPIRE", d.mac_address, predis.TTL)
		if err != nil {
			log.Printf("Set redis ttl error:%v", err)
		}
	}

    return nil 
}

func (d *IPDetails) DNSAsString() string {
	if len(d.DNS) == 0 {
		return "" // 如果没有 DNS 数据，返回空字符串
	}
	var dnsStrings []string
	for _, dns := range d.DNS {
		dnsStrings = append(dnsStrings, dns.String())
	}
	return strings.Join(dnsStrings, ",")
}