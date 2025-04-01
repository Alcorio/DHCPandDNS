package coredns_postgresql

import (
	"fmt"
	"log"
	"strings"
	"time"
	"encoding/json"
	"github.com/coredns/coredns/plugin"

	"github.com/coredns/coredns/plugin/redis"
	"github.com/miekg/dns"

)

var PgUseRedis redis.Redis

func (handler *CoreDNSPostgreSql) findRecord(zone string, name string, types ...string) ([]*Record, error) {
	db, err := handler.db()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	// log.Printf("findRecord: %v, %v", zone, name)
	var query string
	if name != zone {
		query = strings.TrimSuffix(name, "."+zone)
	}
	sqlQuery := fmt.Sprintf("SELECT name, zone, ttl, record_type, content FROM %s WHERE zone = $1 AND name = $2 AND record_type IN ('%s')",
		handler.tableName,
		strings.Join(types, "','"))

	result, err := db.Query(sqlQuery, zone, query)
    // 此处是查看最终的sql语句和查询结果
	// finalSQL := strings.ReplaceAll(sqlQuery, "$1", fmt.Sprintf("'%s'", zone))
	// finalSQL = strings.ReplaceAll(finalSQL, "$2", fmt.Sprintf("'%s'", query))
	// log.Printf("Final SQL Query: %s", finalSQL)
	// log.Printf("---查询%v, %v结果: %v", zone, query, result)
	if err != nil {
		return nil, err
	}

	// columns, err := result.Columns()
	// if err != nil {
	// 	log.Printf("[ERROR] Failed to fetch columns: %v", err)
	// 	return nil, err
	// }
	// log.Printf("[DEBUG]--------------- Columns: %v", columns)

	// // 创建一个切片用于存储每列的值
	// values := make([]interface{}, len(columns))
	// valuePtrs := make([]interface{}, len(columns))
	// for i := range values {
	// 	valuePtrs[i] = &values[i]
	// }

	// // 遍历每一行并打印
	// for result.Next() {
	// 	err := result.Scan(valuePtrs...)
	// 	if err != nil {
	// 		log.Printf("[ERROR] Failed to scan row: %v", err)
	// 		continue
	// 	}

	// 	// 打印每列的值
	// 	row := make(map[string]interface{})
	// 	for i, col := range columns {
	// 		row[col] = values[i]
	// 	}
	// 	log.Printf("[DEBUG] Row: %v", row)
	// }

	var recordName string
	var recordZone string
	var recordType string
	var ttl uint32
	var content string
	records := make([]*Record, 0)
	for result.Next() {
		// log.Printf("扫描层数+1")
		err = result.Scan(&recordName, &recordZone, &ttl, &recordType, &content)
		if err != nil {
			// log.Printf("Scan err!!!!:%v", err)
			return nil, err
		}
		log.Printf("[INFO]:Result: %v, %v, %v, types:%v\n", recordName, recordZone, content, recordType)

		records = append(records, &Record{
			Name:       recordName,
			Zone:       recordZone,
			RecordType: recordType,
			Ttl:        ttl,
			Content:    content,
			handler:    handler,
		})
	}
	// log.Printf("++++++最终结果：%v", records)

	return records, nil
}

func (handler *CoreDNSPostgreSql) loadZones() error {
	db, err := handler.db()
	if err != nil {
		return err
	}
	defer db.Close()

	result, err := db.Query("SELECT DISTINCT zone FROM " + handler.tableName)
	if err != nil {
		return err
	}

	var zone string
	zones := make([]string, 0)
	for result.Next() {
		err = result.Scan(&zone)
		if err != nil {
			return err
		}

		zones = append(zones, zone)
	}

	handler.lastZoneUpdate = time.Now()
	handler.zones = zones

	return nil
}

func (handler *CoreDNSPostgreSql) hosts(zone string, name string) ([]dns.RR, error) {
	recs, err := handler.findRecord(zone, name, "A", "AAAA", "CNAME")
	if err != nil {
		return nil, err
	}

	answers := make([]dns.RR, 0)

	for _, rec := range recs {
		switch rec.RecordType {
		case "A":
			aRec, _, err := rec.AsARecord()
			if err != nil {
				return nil, err
			}
			answers = append(answers, aRec)
		case "AAAA":
			aRec, _, err := rec.AsAAAARecord()
			if err != nil {
				return nil, err
			}
			answers = append(answers, aRec)
		case "CNAME":
			aRec, _, err := rec.AsCNAMERecord()
			if err != nil {
				return nil, err
			}
			answers = append(answers, aRec)
		}
	}

	return answers, nil
}
// 新增代码

func (handler *CoreDNSPostgreSql) findAllRecords(zone string) ([]*Record, error) {
	db, err := handler.db()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	sqlQuery := fmt.Sprintf("SELECT name, zone, ttl, record_type, content FROM %s WHERE zone = $1 ",
		handler.tableName)

	result, err := db.Query(sqlQuery, zone)

	if err != nil {
		return nil, err
	}

	var recordName string
	var recordZone string
	var recordType string
	var ttl uint32
	var content string
	records := make([]*Record, 0)
	for result.Next() {
		// log.Printf("扫描层数+1")
		err = result.Scan(&recordName, &recordZone, &ttl, &recordType, &content)
		if err != nil {
			// log.Printf("Scan err!!!!:%v", err)
			return nil, err
		}
		log.Printf("Result: %v, %v, %v\n", recordName, recordZone, content)
		records = append(records, &Record{
			Name:       recordName,
			Zone:       recordZone,
			RecordType: recordType,
			Ttl:        ttl,
			Content:    content,
			handler:    handler,
		})
	}
	return records, nil
}

// 新增代码
func (handler *CoreDNSPostgreSql) updateRedis(records []*Record) error {
    
	zoneMap := make(map[string]map[string]*redis.Record)

	for _, record := range records {
		zoneKey := strings.TrimSpace(record.Zone)

		if _, ok := zoneMap[zoneKey]; !ok {
			zoneMap[zoneKey] = make(map[string]*redis.Record)
		}

		nameKey := strings.TrimSpace(record.Name)
		if nameKey == ""{
			nameKey = "@"
		}

		if _, ok := zoneMap[zoneKey][nameKey]; !ok {
			zoneMap[zoneKey][nameKey] = &redis.Record{}
		}

		// 解析record.Content
		switch strings.ToUpper(record.RecordType){
		    case "A":
				// A记录，比如Content={"ip":"1.1.1.3"}
				var aValue redis.A_Record
				if err := json.Unmarshal([]byte(record.Content), &aValue); err != nil {
					log.Printf("failed to parse A content: %v, content=%s", err, record.Content)
					continue
				}
				aValue.Ttl = record.Ttl
				zoneMap[zoneKey][nameKey].A = append(zoneMap[zoneKey][nameKey].A, aValue)

		    case "AAAA":
				// AAAA记录，Content = {"ip":"::1"}
				var aaaaValue redis.AAAA_Record
				if err := json.Unmarshal([]byte(record.Content), &aaaaValue); err != nil {
					log.Printf("failed to parse AAAA content: %v, content=%s", err, record.Content)
					continue
				}
				aaaaValue.Ttl = record.Ttl
				zoneMap[zoneKey][nameKey].AAAA = append(zoneMap[zoneKey][nameKey].AAAA, aaaaValue)
			
		    case "CNAME":
				// CNAME记录，Content = {"host":"a.example.org."}
				var cnameValue redis.CNAME_Record
				if err := json.Unmarshal([]byte(record.Content), &cnameValue); err != nil {
					log.Printf("failed to parse CNAME content: %v, content=%s", err, record.Content)
					continue
				}
				cnameValue.Ttl = record.Ttl
				zoneMap[zoneKey][nameKey].CNAME = append(zoneMap[zoneKey][nameKey].CNAME, cnameValue)
			
			case "TXT":
				// TXT记录，Content = {"text":"hello"}
				var textValue redis.TXT_Record
				if err := json.Unmarshal([]byte(record.Content), &textValue); err != nil {
					log.Printf("failed to parse TXT content: %v, content=%s", err, record.Content)
					continue
				}
				textValue.Ttl = record.Ttl
				zoneMap[zoneKey][nameKey].TXT = append(zoneMap[zoneKey][nameKey].TXT, textValue)

			case "NS":
				// NS记录，Content = {"host":"ns1.example.org."}
				var nsValue redis.NS_Record
				if err := json.Unmarshal([]byte(record.Content), &nsValue); err != nil {
					log.Printf("failed to parse NS content: %v, content=%s", err, record.Content)
					continue
				}
				nsValue.Ttl = record.Ttl
				zoneMap[zoneKey][nameKey].NS = append(zoneMap[zoneKey][nameKey].NS, nsValue)
			
			case "MX":
				// MX记录，Content = {"host:"mail.example.org","preference":10 }
				var mxValue redis.MX_Record
				if err := json.Unmarshal([]byte(record.Content), &mxValue); err != nil {
					log.Printf("failed to parse MX content: %v, content=%s", err, record.Content)
					continue
				}
				mxValue.Ttl = record.Ttl
				zoneMap[zoneKey][nameKey].MX = append(zoneMap[zoneKey][nameKey].MX, mxValue)

			case "SRV":
				// SRV记录，Content = {"target":"tcp.example.com.","port":123,"priority":10,"weight":100}
				var srvValue redis.SRV_Record
				if err := json.Unmarshal([]byte(record.Content), &srvValue); err != nil {
					log.Printf("failed to parse MX content: %v, content=%s", err, record.Content)
					continue
				}
				srvValue.Ttl = record.Ttl
				zoneMap[zoneKey][nameKey].SRV = append(zoneMap[zoneKey][nameKey].SRV, srvValue)

			case "SOA": 
			    // pg_test已验证MBox，mbox，Mbox等大小写不一致的情况也是能强制匹配的
				// SOA记录，Content = {"ns":"ns1.example.org","mbox":"hostmaster.example.org.","refresh":3600,"retry":600,"expire":86400,"minttl":300}
                // 单位秒，86400 一天 1209600 14天
                var soaValue redis.SOA_Record
				if err := json.Unmarshal([]byte(record.Content), &soaValue); err != nil {
					log.Printf("failed to parse SOA content: %v, content=%s", err, record.Content)
					continue
				}
				soaValue.Ttl = record.Ttl
				zoneMap[zoneKey][nameKey].SOA = soaValue
		    
			case "CAA":
				// CAA记录，Content = {"flag":0,"tag":"issue","value":"letsencrypt.org"}
				var caaValue redis.CAA_Record
				if err := json.Unmarshal([]byte(record.Content), &caaValue); err != nil {
					log.Printf("failed to parse CAA content: %v, content=%s", err, record.Content)
					continue
				}
				zoneMap[zoneKey][nameKey].CAA = append(zoneMap[zoneKey][nameKey].CAA, caaValue)
			
			case "AXFR":
				// 这里pg插件不用完成，交给redis
				log.Printf("AXFR will be achieved by redis")
			default:
				// 其他类型忽略
				log.Printf("unknown record type %s, skip\n", record.RecordType)
		}

		for zone, names := range zoneMap {
			for name, redisEntry := range names {
				if redisEntry.IsEmpty() {
					log.Printf("redisEntry is empty:%v", redisEntry)
					continue 
				}
				b, err := json.Marshal(redisEntry)
				if err != nil {
					log.Printf("failed to marshal redisEntry:%v, entry=%+v", err, redisEntry)
					continue
				}

				if handler.redisTtl > 0 {
					err = PgUseRedis.SavePlus(zone, name, string(b), handler.redisTtl)
				} else {
					err = PgUseRedis.Save(zone, name, string(b))
				}
				

				if err != nil {
					return fmt.Errorf("failed to HSET zone=%v, name=%v, data=%v: %v", zone, name, string(b), err)
				}
			}
		}
	}
	return nil
}
// 新增如果A记录找不到通过cname查找并返回A记录
func (handler *CoreDNSPostgreSql) findAbyCNAME(content string) ( []*Record, error) {
	var result map[string]string
	err := json.Unmarshal([]byte(content), &result)
    if err != nil {
		return nil, fmt.Errorf("findAbyCNAME Unmarshal error:", err)
	}

	hostvalue, exist := result["host"]
	if exist {
		qzone := plugin.Zones(handler.zones).Matches(hostvalue)
		fmt.Println("findAbyCNAME new zone:", qzone)
		ans, err := handler.findRecord(qzone, hostvalue, "A")
        if err != nil {
			return nil, fmt.Errorf("findAbyCNAME findrecord error:", err)
		}
		return ans, nil
	} else {
		fmt.Println("host key does not exist")
		return nil, fmt.Errorf("cname host key non exist")
	}
}

