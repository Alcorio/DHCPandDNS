package coredns_postgresql

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/redis"
)

const (
	defaultTtl                = 360
	defaultMaxLifeTime        = 1 * time.Minute
	defaultMaxOpenConnections = 10
	defaultMaxIdleConnections = 10
	defaultZoneUpdateTime     = 10 * time.Minute
	defaultRedisOn            = false
	defaultBatchUpdate        = true 
	defaultRedisTtl           = -1
)

// func init() {
// 	caddy.RegisterPlugin("postgresql", caddy.Plugin{
// 		ServerType: "dns",
// 		Action:     setup,
// 	})
// }
func init() { plugin.Register("postgresql", setup) }

func setup(c *caddy.Controller) error {
	r, err := postgresqlParse(c)
	if err != nil {
		return plugin.Error("postgresql", err)
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		r.Next = next
		return r
	})

	return nil
}

func postgresqlParse(c *caddy.Controller) (*CoreDNSPostgreSql, error) {
	postgresql := CoreDNSPostgreSql{
		TablePrefix: "coredns_",
		Ttl:         300,
		redisOn:      false,
		redisTtl:    -1,  
        batchUpdateRedis:  true,
	}
	var err error

	c.Next()
	if c.NextBlock() {
		for {
			switch c.Val() {
			case "datasource":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				postgresql.Datasource = c.Val()
			case "table_prefix":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				postgresql.TablePrefix = c.Val()
			case "max_lifetime":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				var val time.Duration
				val, err = time.ParseDuration(c.Val())
				if err != nil {
					val = defaultMaxLifeTime
				}
				postgresql.MaxLifetime = val
			case "max_open_connections":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				var val int
				val, err = strconv.Atoi(c.Val())
				if err != nil {
					val = defaultMaxOpenConnections
				}
				postgresql.MaxOpenConnections = val
			case "max_idle_connections":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				var val int
				val, err = strconv.Atoi(c.Val())
				if err != nil {
					val = defaultMaxIdleConnections
				}
				postgresql.MaxIdleConnections = val
			case "zone_update_interval":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				var val time.Duration
				val, err = time.ParseDuration(c.Val())
				if err != nil {
					val = defaultZoneUpdateTime
				}
				postgresql.zoneUpdateTime = val
			case "ttl":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				var val int
				val, err = strconv.Atoi(c.Val())
				if err != nil {
					val = defaultTtl
				}
				postgresql.Ttl = uint32(val)
			case "redisOn":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				var val bool
				val, err = strconv.ParseBool(c.Val())  // 只能为ture, 0, 1等bool类型
				if err != nil {  // 否则默认false
					val = defaultRedisOn
				}
				postgresql.redisOn = val
			case "redisTtl":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				var val time.Duration
				val, err = time.ParseDuration(c.Val())
				if err != nil {
					val = defaultRedisTtl
				}
				postgresql.redisTtl = val
			case "batchUpdateRedis":
				if !c.NextArg() {
					return &CoreDNSPostgreSql{}, c.ArgErr()
				}
				var val bool
				val, err = strconv.ParseBool(c.Val()) // 同上
				if err != nil {
					val = defaultBatchUpdate
				}
				postgresql.batchUpdateRedis = val
			default:
				if c.Val() != "}" {
					return &CoreDNSPostgreSql{}, c.Errf("unknown property '%s'", c.Val())
				}
			}

			if !c.Next() {
				break
			}
		}

	}
    if postgresql.redisOn {
		err := PgUseRedis.SetRedisConfig(redis.SharedRedisConfig)
		if err != nil {
			fmt.Printf("pgUseRedis err, module canceled:%v", err) 
			postgresql.redisOn = false 
		} else {
			PgUseRedis.Connect()
		}
	}


	db, err := postgresql.db()
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	postgresql.tableName = postgresql.TablePrefix + "records"

	return &postgresql, nil
}

func (handler *CoreDNSPostgreSql) db() (*sql.DB, error) {
	db, err := sql.Open("postgres", handler.Datasource)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(handler.MaxLifetime)
	db.SetMaxOpenConns(handler.MaxOpenConnections)
	db.SetMaxIdleConns(handler.MaxIdleConnections)

	return db, nil
}
