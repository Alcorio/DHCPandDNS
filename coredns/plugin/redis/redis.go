package redis

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"log"
	"strconv"

	"github.com/miekg/dns"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/fall"

	redisCon "github.com/gomodule/redigo/redis"
)

var SharedRedisConfig map[string]string // 新增，用以pg插件

type Redis struct {
	Next           plugin.Handler
	Pool           *redisCon.Pool
	redisAddress   string
	redisPassword  string
	connectTimeout int
	readTimeout    int
	keyPrefix      string
	keySuffix      string
	Ttl            uint32   // ttl default ttl for dns records, 300 if not provided
	Zones          []string
	LastZoneUpdate time.Time

	Fall           fall.F
	pgBatchUpdate  bool 
}

func (redis *Redis) LoadZones() {
	var (
		reply interface{}
		err error
		zones []string
	)

	conn := redis.Pool.Get()
	if conn == nil {
		fmt.Println("error connecting to redis")
		return
	}
	defer conn.Close()

	reply, err = conn.Do("KEYS", redis.keyPrefix + "*" + redis.keySuffix)
	if err != nil {
		return
	}
	// 转换成字符串数组
	zones, err = redisCon.Strings(reply, nil)
	if err != nil {
		log.Printf("redis func loadzones err:%v", err)
	}
	for i, _ := range zones { // 再去掉前缀和后缀
		zones[i] = strings.TrimPrefix(zones[i], redis.keyPrefix)
		zones[i] = strings.TrimSuffix(zones[i], redis.keySuffix)
	}
	redis.LastZoneUpdate = time.Now()  // 记录下更新时间
	redis.Zones = zones
}

func (redis *Redis) A(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, a := range record.A {
		if a.Ip == nil {
			continue
		}
		r := new(dns.A)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeA,
			Class: dns.ClassINET, Ttl: redis.minTtl(a.Ttl)}
		r.A = a.Ip
		answers = append(answers, r)
	}
	return
}

func (redis Redis) AAAA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, aaaa := range record.AAAA {
		if aaaa.Ip == nil {
			continue
		}
		r := new(dns.AAAA)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeAAAA,
			Class: dns.ClassINET, Ttl: redis.minTtl(aaaa.Ttl)}
		r.AAAA = aaaa.Ip
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) CNAME(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, cname := range record.CNAME {
		if len(cname.Host) == 0 {
			continue
		}
		r := new(dns.CNAME)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeCNAME,
			Class: dns.ClassINET, Ttl: redis.minTtl(cname.Ttl)}
		r.Target = dns.Fqdn(cname.Host)
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) TXT(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, txt := range record.TXT {
		if len(txt.Text) == 0 {
			continue
		}
		r:= new(dns.TXT)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeTXT,
			Class: dns.ClassINET, Ttl: redis.minTtl(txt.Ttl)}
		r.Txt = split255(txt.Text)
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) NS(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, ns := range record.NS {
		if len(ns.Host) == 0 {
			continue
		}
		r := new(dns.NS)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeNS,
			Class: dns.ClassINET, Ttl: redis.minTtl(ns.Ttl)}
		r.Ns = ns.Host
		answers = append(answers, r)
		extras = append(extras, redis.hosts(ns.Host, z)...)
	}
	return
}

func (redis *Redis) MX(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, mx := range record.MX {
		if len(mx.Host) == 0 {
			continue
		}
		r := new(dns.MX)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeMX,
			Class: dns.ClassINET, Ttl: redis.minTtl(mx.Ttl)}
		r.Mx = mx.Host
		r.Preference = mx.Preference
		answers = append(answers, r)
		extras = append(extras, redis.hosts(mx.Host, z)...)
	}
	return
}

func (redis *Redis) SRV(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, srv := range record.SRV {
		if len(srv.Target) == 0 {
			continue
		}
		r := new(dns.SRV)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeSRV,
			Class: dns.ClassINET, Ttl: redis.minTtl(srv.Ttl)}
		r.Target = srv.Target
		r.Weight = srv.Weight
		r.Port = srv.Port
		r.Priority = srv.Priority
		answers = append(answers, r)
		extras = append(extras, redis.hosts(srv.Target, z)...)
	}
	return
}

func (redis *Redis) SOA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	r := new(dns.SOA)
	if record.SOA.Ns == "" {
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeSOA,
			Class: dns.ClassINET, Ttl: redis.Ttl}
		r.Ns = "ns1." + name
		r.Mbox = "hostmaster." + name
		r.Refresh = 86400
		r.Retry = 7200
		r.Expire = 3600
		r.Minttl = redis.Ttl
	} else {
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(z.Name), Rrtype: dns.TypeSOA,
			Class: dns.ClassINET, Ttl: redis.minTtl(record.SOA.Ttl)}
		r.Ns = record.SOA.Ns
		r.Mbox = record.SOA.MBox
		r.Refresh = record.SOA.Refresh
		r.Retry = record.SOA.Retry
		r.Expire = record.SOA.Expire
		r.Minttl = record.SOA.MinTtl
	}
	r.Serial = redis.serial()
	answers = append(answers, r)
	return
}

func (redis *Redis) CAA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	for _, caa := range record.CAA {
		if caa.Value == "" || caa.Tag == ""{
			continue
		}
		r := new(dns.CAA)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeCAA, Class: dns.ClassINET}
		r.Flag = caa.Flag
		r.Tag = caa.Tag
		r.Value = caa.Value
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) AXFR(z *Zone) (records []dns.RR) {
	//soa, _ := redis.SOA(z.Name, z, record)
	soa := make([]dns.RR, 0)
	answers := make([]dns.RR, 0, 10)
	extras := make([]dns.RR, 0, 10)

	// Allocate slices for rr Records
	records = append(records, soa...)
	for key := range z.Locations {
		if key == "@"  {
			location := redis.findLocation(z.Name, z)
			record := redis.get(location, z)
			soa, _ = redis.SOA(z.Name, z, record)
		} else {
			fqdnKey := dns.Fqdn(key) + z.Name
			var as []dns.RR
			var xs []dns.RR

			location := redis.findLocation(fqdnKey, z)
			record := redis.get(location, z)

			// Pull all zone records
			as, xs = redis.A(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.AAAA(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.CNAME(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.MX(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.SRV(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.TXT(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)
		}
	}

	records = soa
	records = append(records, answers...)
	records = append(records, extras...)
	records = append(records, soa...)

	fmt.Println(records)
 	return
}

func (redis *Redis) hosts(name string, z *Zone) []dns.RR {
	var (
		record *Record
		answers []dns.RR
	)
	location := redis.findLocation(name, z)
	if location == "" {
		return nil
	}
	record = redis.get(location, z)
	a, _ := redis.A(name, z, record)
	answers = append(answers, a...)
	aaaa, _ := redis.AAAA(name, z, record)
	answers = append(answers, aaaa...)
	cname, _ := redis.CNAME(name, z, record)
	answers = append(answers, cname...)
	return answers
}

func (redis *Redis) serial() uint32 {
	return uint32(time.Now().Unix())
}

func (redis *Redis) minTtl(ttl uint32) uint32 {
	if redis.Ttl == 0 && ttl == 0 {
		return defaultTtl
	}
	if redis.Ttl == 0 {
		return ttl
	}
	if ttl == 0 {
		return redis.Ttl
	}
	if redis.Ttl < ttl {
		return redis.Ttl
	}
	return  ttl
}

func (redis *Redis) findLocation(query string, z *Zone) string { // z是传入的该zone下的所有信息
	var (
		ok bool
		closestEncloser, sourceOfSynthesis string
	)

	// request for zone records
	if query == z.Name { // 字段等于zone本身
		return query
	}
   //  log.Println("执行查找")
	query = strings.TrimSuffix(query, "." + z.Name)  // 去掉后缀
	// 假如输入example.com   而zone是example.com. 不会变

	if _, ok = z.Locations[query]; ok {  // 子域名如果直接存在数据库，直接返回
		return query
	}
    // 逐层递归匹配
	closestEncloser, sourceOfSynthesis, ok = splitQuery(query) // 为了获取子域名中的上级域名和 *.上级域名 
	for ok { // 递归查找
		ceExists := keyMatches(closestEncloser, z) || keyExists(closestEncloser, z)
		ssExists := keyExists(sourceOfSynthesis, z)
		// log.Printf("ceExists:%v, ssExists:%v",ceExists,ssExists )
		if ceExists { // 子域名的上级域名存在于zone.field
			if ssExists {    // 说明*.上级域名 直接存在    e.g 假如传入p.a.d.baidu.com 匹配 a.d. 和 *.a.d
				return sourceOfSynthesis
			} else { // 说明 上级域名存在，但是*.上级域名不存在 说明数据库没有该完整域名的信息
				return ""
			}
		} else { // 不存在，继续匹配  d. 和 *.d. 还是不存在 则 "" 和 "*" 此时再去匹配空字段和*字段，已经是最顶级的域名了，没有最后返回空
			closestEncloser, sourceOfSynthesis, ok = splitQuery(closestEncloser) // 如果没有匹配到再细分
		}
	}
	return ""
}

func (redis *Redis) get(key string, z *Zone) *Record {
	var (
		err error
		reply interface{}
		val string
	)
	conn := redis.Pool.Get()
	if conn == nil {
		fmt.Println("error connecting to redis")
		return nil
	}
	defer conn.Close()

	var label string
	if key == z.Name {
		label = "@"
	} else {
		label = key
	}

	reply, err = conn.Do("HGET", redis.keyPrefix + z.Name + redis.keySuffix, label)
	if err != nil {
		return nil
	}
	val, err = redisCon.String(reply, nil)
	if err != nil {
		return nil
	}
	r := new(Record)
	err = json.Unmarshal([]byte(val), r)
	if err != nil {
		fmt.Println("parse error : ", val, err)
		return nil
	}
	return r
}

func keyExists(key string, z *Zone) bool {
	_, ok := z.Locations[key]
	return ok
}

func keyMatches(key string, z *Zone) bool {
	for value := range z.Locations {
		if strings.HasSuffix(value, key) {
			return true
		}
	}
	return false
}

func splitQuery(query string) (string, string, bool) {
	if query == "" {
		return "", "", false
	}
	var (
		splits []string
		closestEncloser string   // 存储域名中的剩余部分。
		sourceOfSynthesis string // 存储合成域名
	)
	splits = strings.SplitAfterN(query, ".", 2) // 按照第一个出现"." 分成最多两个字符串
	if len(splits) == 2 {  // 如果是两部分，解析分为，一组上级域名，一组通配符.上级域名
		closestEncloser = splits[1]
		sourceOfSynthesis = "*." + closestEncloser
	} else { // 如果为1，没有上级域名了，保留一个通配符*
		closestEncloser = ""
		sourceOfSynthesis = "*"
	}
	log.Printf("解析过程：close %v, source %v", closestEncloser, sourceOfSynthesis)
	return closestEncloser, sourceOfSynthesis, true
}

func (redis *Redis) Connect() {
	redis.Pool = &redisCon.Pool{
		Dial: func () (redisCon.Conn, error) {
			opts := []redisCon.DialOption{}
			if redis.redisPassword != "" {
				opts = append(opts, redisCon.DialPassword(redis.redisPassword))
			}
			if redis.connectTimeout != 0 {
				opts = append(opts, redisCon.DialConnectTimeout(time.Duration(redis.connectTimeout)*time.Millisecond))
			}
			if redis.readTimeout != 0 {
				opts = append(opts, redisCon.DialReadTimeout(time.Duration(redis.readTimeout)*time.Millisecond))
			}

			return redisCon.Dial("tcp", redis.redisAddress, opts...)
		},
	}
}

func (redis *Redis) Save(zone string, subdomain string, value string) error {
	var err error

	conn := redis.Pool.Get()
	if conn == nil {
		fmt.Println("error connecting to redis")
		return nil
	}
	defer conn.Close()

	_, err = conn.Do("HSET", redis.keyPrefix + zone + redis.keySuffix, subdomain, value)
	return err
}

func (redis *Redis) load(zone string) *Zone {
	var (
		reply interface{}
		err error
		vals []string
	)

	conn := redis.Pool.Get()
	if conn == nil {
		fmt.Println("error connecting to redis")
		return nil
	}
	defer conn.Close()

	reply, err = conn.Do("HKEYS", redis.keyPrefix + zone + redis.keySuffix)
	if err != nil {
		return nil
	}
	z := new(Zone)
	z.Name = zone
	vals, err = redisCon.Strings(reply, nil)
	if err != nil {
		return nil
	}
	z.Locations = make(map[string]struct{})
	for _, val := range vals {
		z.Locations[val] = struct{}{}
	}

	return z
}

func split255(s string) []string {
	if len(s) < 255 {
		return []string{s}
	}
	sx := []string{}
	p, i := 0, 255
	for {
		if i <= len(s) {
			sx = append(sx, s[p:i])
		} else {
			sx = append(sx, s[p:])
			break

		}
		p, i = p+255, i+255
	}

	return sx
}

const (
	defaultTtl = 360
	hostmaster = "hostmaster"
	zoneUpdateTime = 10*time.Minute
	transferLength = 1000
	defaultBatchUpdate = true
)


// 新增代码，用以pg插件更新配置
// Setter 方法
func (p *Redis) SetRedisConfig(r map[string]string) error{

	p.redisPassword = r["redisPassword"]
	p.redisAddress = r["redisAddress"]
	p.keyPrefix = r["keyPrefix"]
	p.keySuffix = r["keySuffix"]
	if connectTimeout, err := strconv.Atoi(r["connectTimeout"]); err == nil {
		p.connectTimeout = connectTimeout
	} else {
		return fmt.Errorf("SetRedisConfig err:%v", err)
	}

	if timeout, err := strconv.Atoi(r["readTimeout"]); err == nil {
		p.readTimeout = timeout
	} else {
	    return fmt.Errorf("SetRedisConfig err:%v", err)
	}

	if parsedTtl, err := strconv.Atoi(r["ttl"]); err == nil {
		p.Ttl = uint32(parsedTtl)
	} else {
		return fmt.Errorf("SetRedisConfig err:%v", err)
	}
	return nil
}

// 新增代码支持设置ttl
func (redis *Redis) SavePlus(zone string, subdomain string, value string, t time.Duration) error {
	var err error

	conn := redis.Pool.Get()
	if conn == nil {
		fmt.Println("error connecting to redis")
		return nil
	}
	defer conn.Close()

	_, err = conn.Do("HSET", redis.keyPrefix + zone + redis.keySuffix, subdomain, value)
	if err != nil {
		return fmt.Errorf("HSET failed:%v", err)
	}
	_, err = conn.Do("EXPIRE", redis.keyPrefix + zone + redis.keySuffix, t)
    if err != nil {
		return fmt.Errorf("EXPIRE failed:%v", err)
	}
	return err
}