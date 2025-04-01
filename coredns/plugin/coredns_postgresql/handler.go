package coredns_postgresql

import (
	"log"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	_ "github.com/lib/pq"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

type CoreDNSPostgreSql struct {
	Next               plugin.Handler
	Datasource         string
	TablePrefix        string
	MaxLifetime        time.Duration
	MaxOpenConnections int
	MaxIdleConnections int
	Ttl                uint32

	tableName      string
	lastZoneUpdate time.Time
	zoneUpdateTime time.Duration
	zones          []string

	redisOn          bool          // 默认false
	batchUpdateRedis bool          // 默认true
	redisTtl         time.Duration // redis插件里的ttl是用来设置dns记录的默认ttl，此处的ttl用来设置数据库存储数据的时间 默认-1
}

// ServeDNS implements the plugin.Handler interface.
func (handler *CoreDNSPostgreSql) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

	qName := state.Name()
	qType := state.Type()
	remoteAddr := w.RemoteAddr().String()

	log.Printf("[DEBUG] pg Received DNS query: ip:%v, Name=%s, Type=%s", remoteAddr, qName, qType)
	if time.Since(handler.lastZoneUpdate) > handler.zoneUpdateTime {
		//log.Printf("[DEBUG] Reloading zones, last update: %s", handler.lastZoneUpdate)
		err := handler.loadZones()
		if err != nil {
			//log.Printf("[ERROR] Failed to load zones: %v", err)
			return handler.errorResponse(state, dns.RcodeServerFailure, err)
		}
	}
	log.Printf(", Zones=%s end.\n", handler.zones)
	qZone := plugin.Zones(handler.zones).Matches(qName)
	if qZone == "" {
		//log.Printf("[DEBUG] No matching zone found for query: %s", qName)
		log.Printf("[DEBUG] pg没有找到zones->进入上游插件")
		return plugin.NextOrFailure(handler.Name(), handler.Next, ctx, w, r)

	}
	//log.Printf("[DEBUG] Query matched zone: %s", qZone)
	//log.Printf("pg查找qzone:%v, qname:%v, qType:%v", qZone, qName, qType)
	records, err := handler.findRecord(qZone, qName, qType)
	if err != nil {
		return handler.errorResponse(state, dns.RcodeServerFailure, err)
	}

	var appendSOA bool
	if len(records) == 0 {
		appendSOA = true
		// no record found but we are going to return a SOA
		recs, err := handler.findRecord(qZone, "", "SOA")
		if err != nil {
			return handler.errorResponse(state, dns.RcodeServerFailure, err)
		}
		records = append(records, recs...)
        // 新增代码，支持找不到A记录时，通过cname一次查找A记录
		// if qType == "A" {
		// 	log.Printf("No A records, but searching CNAME")
		// 	recs, err = handler.findRecord(qZone, qName, "CNAME")
		// 	if err != nil {
		// 		return handler.errorResponse(state, dns.RcodeServerFailure, err)
		// 	}
		// 	if len(recs) != 0 {
		// 		ans, err := handler.findAbyCNAME(recs[0].Content)
		// 		if err != nil {
		// 			log.Println(err)
		// 		}
		// 		records = append(records, ans...)

		// 	} else {
		// 		log.Println("no CNAME records")
		// 	}
			
		// }

	}

	if qType == "AXFR" { // AXFR 区域传输不支持, 更新redis，并在下次通过redis进行传输
		if !handler.redisOn {
			log.Printf("Pg don't support AXFR, please open both redis and pg.")
			return handler.errorResponse(state, dns.RcodeNotImplemented, nil)
		}
		answers, err := handler.findAllRecords(qZone)
		if err != nil {
			log.Printf("AXFR Getall records failed:%v", err)
			return handler.errorResponse(state, dns.RcodeServerFailure, nil)
		}
		err = handler.updateRedis(answers)
		if err != nil {
			log.Printf("AXFR update redis failed:%v", err)
			return handler.errorResponse(state, dns.RcodeServerFailure, nil)
		}
		log.Printf("Ok, all data updated in redis, please AXFR again by redis.")
		return handler.errorResponse(state, dns.RcodeNotImplemented, nil)
	}

	answers := make([]dns.RR, 0, 10)
	extras := make([]dns.RR, 0, 10)

	if records != nil && handler.redisOn {
		err := handler.updateRedis(records)
		if err != nil {
			log.Printf("Update redis failed:%v, cancel module", err)
			handler.redisOn = false
		}
		log.Printf("Ok, data updated in redis.")
	}

	for _, record := range records {
		var answer dns.RR
		switch record.RecordType {
		case "A":
			answer, extras, err = record.AsARecord()
		case "AAAA":
			answer, extras, err = record.AsAAAARecord()
		case "CNAME":
			answer, extras, err = record.AsCNAMERecord()
		case "SOA":
			answer, extras, err = record.AsSOARecord()
		case "SRV":
			answer, extras, err = record.AsSRVRecord()
		case "NS":
			answer, extras, err = record.AsNSRecord()
		case "MX":
			answer, extras, err = record.AsMXRecord()
		case "TXT":
			answer, extras, err = record.AsTXTRecord()
		case "CAA":
			answer, extras, err = record.AsCAARecord()
		default:
			return handler.errorResponse(state, dns.RcodeNotImplemented, nil)
		}

		if err != nil {
			return handler.errorResponse(state, dns.RcodeServerFailure, err)
		}
		if answer != nil {
			answers = append(answers, answer)
		}
	}

	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true
	m.RecursionAvailable = false
	m.Compress = true

	if !appendSOA {
		m.Answer = append(m.Answer, answers...)
	} else {
		m.Ns = append(m.Ns, answers...)
	}
	m.Extra = append(m.Extra, extras...)

	state.SizeAndDo(m)
	m = state.Scrub(m)
	_ = w.WriteMsg(m)
	return dns.RcodeSuccess, nil
}

// Name implements the Handler interface.
func (handler *CoreDNSPostgreSql) Name() string { return "postgresql" }

func (handler *CoreDNSPostgreSql) errorResponse(state request.Request, rCode int, err error) (int, error) {
	m := new(dns.Msg)
	m.SetRcode(state.Req, rCode)
	m.Authoritative, m.RecursionAvailable, m.Compress = true, false, true

	state.SizeAndDo(m)
	_ = state.W.WriteMsg(m)
	// Return success as the rCode to signal we have written to the client.
	return dns.RcodeSuccess, err
}
