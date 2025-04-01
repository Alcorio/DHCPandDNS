package redis

import (
	"fmt"
	// "fmt"
	"time"
    //"log"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

// ServeDNS implements the plugin.Handler interface.
func (redis *Redis) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

	qname := state.Name()
	qtype := state.Type()
	//log.Printf("测试验证进行redis处理")
    
	fmt.Printf("redis插件处理: qname = %v, qtype = %v", qname, qtype)
	// 新增代码传递实例到下一个插件 但是不推荐这么做，因为传递数据最好轻量
	// ctx = context.WithValue(ctx, "redis_instance", redis)

	if time.Since(redis.LastZoneUpdate) > zoneUpdateTime {
		redis.LoadZones()  // 如果更新redis.zones的时间距离上次超过10分钟，重新获取一遍
	}

	zone := plugin.Zones(redis.Zones).Matches(qname)  // 匹配父域名和子域名 同时解析标签个数来判断
	fmt.Printf(", zone = %v end.\n", zone)
	if zone == "" { // redis没有父域名
		return plugin.NextOrFailure(qname, redis.Next, ctx, w, r)
	}

	z := redis.load(zone)  // 找到zone的所有field，并初始化为空结构体
	if z == nil {
		return redis.errorResponse(state, zone, dns.RcodeServerFailure, nil)
	}

	if qtype == "AXFR" {  // 区域传输功能，redis支持，但是pg插件目前不支持
		records := redis.AXFR(z)

		ch := make(chan *dns.Envelope)   // dns.Envelope是dns的传输单元
		tr := new(dns.Transfer)
		tr.TsigSecret = nil   // 表示没有启用 TSIG（Transaction Signature，DNS 事务签名）认证。

		go func(ch chan *dns.Envelope) {
			j, l := 0, 0

			for i, r := range records {
				l += dns.Len(r)
				if l > transferLength {
					ch <- &dns.Envelope{RR: records[j:i]}
					l = 0
					j = i
				}
			}
			if j < len(records) {
				ch <- &dns.Envelope{RR: records[j:]}
			}
			close(ch)
		}(ch)

		err := tr.Out(w, r, ch)  
		if err != nil {
			fmt.Println(err)
		}
		w.Hijack()  // 劫持 HTTP 连接。劫持后，HTTP 底层的 TCP 连接可由程序直接控制，适用于长连接（如 AXFR
		return dns.RcodeSuccess, nil
	}
	// fmt.Printf("qname and z: %v, %v\n",qname, z)

	location := redis.findLocation(qname, z) 
	if len(location) == 0 { // empty, no results
		// fmt.Printf("没有找到field\n")
		if redis.Fall.Through(qname){ // 新增fallthrough方法 遇到特定域名再去查找pg 
			// 因为match函数是解析父域名和子域名，所以默认值根域名时，所有查询都会进入
			return plugin.NextOrFailure(qname, redis.Next, ctx, w, r)
		}
		return redis.errorResponse(state, zone, dns.RcodeNameError, nil)
	}

	answers := make([]dns.RR, 0, 10)
	extras := make([]dns.RR, 0, 10)

	record := redis.get(location, z)  // zone记录的location(也就是field)，反序列化为record结构体
    fmt.Printf("找到了记录：%v\n", record)
	switch qtype {
	case "A":
		answers, extras = redis.A(qname, z, record)
	case "AAAA":
		answers, extras = redis.AAAA(qname, z, record)
	case "CNAME":
		answers, extras = redis.CNAME(qname, z, record)
	case "TXT":
		answers, extras = redis.TXT(qname, z, record)
	case "NS":
		answers, extras = redis.NS(qname, z, record)
	case "MX":
		answers, extras = redis.MX(qname, z, record)
	case "SRV":
		answers, extras = redis.SRV(qname, z, record)
	case "SOA":
		answers, extras = redis.SOA(qname, z, record)
	case "CAA":
		answers, extras = redis.CAA(qname, z, record)

	default:
		return redis.errorResponse(state, zone, dns.RcodeNotImplemented, nil)
	}

    // 新增代码 pg单条更新redis时 redis如果存在域名但找不到单条数据，还是需要去找pg
	if !redis.pgBatchUpdate && len(answers) == 0{
		return plugin.NextOrFailure(qname, redis.Next, ctx, w, r)
	}


	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative, m.RecursionAvailable, m.Compress = true, false, true

	m.Answer = append(m.Answer, answers...)
	m.Extra = append(m.Extra, extras...)

	state.SizeAndDo(m)
	m = state.Scrub(m)
	_ = w.WriteMsg(m)
	return dns.RcodeSuccess, nil
}

// Name implements the Handler interface.
func (redis *Redis) Name() string { return "redis" }

func (redis *Redis) errorResponse(state request.Request, zone string, rcode int, err error) (int, error) {
	m := new(dns.Msg)
	m.SetRcode(state.Req, rcode)
	m.Authoritative, m.RecursionAvailable, m.Compress = true, false, true

	state.SizeAndDo(m)
	_ = state.W.WriteMsg(m)
	// Return success as the rcode to signal we have written to the client.
	return dns.RcodeSuccess, err
}
