package redis

import (
	"strconv"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
)

func init() {
	caddy.RegisterPlugin("redis", caddy.Plugin{
		ServerType: "dns",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	r, err := redisParse(c)
	if err != nil {
		return plugin.Error("redis", err)
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		r.Next = next
		return r
	})

	return nil
}

func redisParse(c *caddy.Controller) (*Redis, error) {
	redis := Redis{
		keyPrefix:     "",
		keySuffix:     "",
		Ttl:           300,
		pgBatchUpdate: true,
	}
	var (
		err error
	)
	// log.Infof("测试验证安装redis by infof")  // 说明安装顺序确实按照corefile顺序来的
	for c.Next() {
		if c.NextBlock() {
			for {
				switch c.Val() {
				case "address":
					if !c.NextArg() {
						return &Redis{}, c.ArgErr()
					}
					redis.redisAddress = c.Val()
				case "password":
					if !c.NextArg() {
						return &Redis{}, c.ArgErr()
					}
					redis.redisPassword = c.Val()
				case "prefix":
					if !c.NextArg() {
						return &Redis{}, c.ArgErr()
					}
					redis.keyPrefix = c.Val()
				case "suffix":
					if !c.NextArg() {
						return &Redis{}, c.ArgErr()
					}
					redis.keySuffix = c.Val()
				case "connect_timeout":
					if !c.NextArg() {
						return &Redis{}, c.ArgErr()
					}
					redis.connectTimeout, err = strconv.Atoi(c.Val())
					if err != nil {
						redis.connectTimeout = 0
					}
				case "read_timeout":
					if !c.NextArg() {
						return &Redis{}, c.ArgErr()
					}
					redis.readTimeout, err = strconv.Atoi(c.Val())
					if err != nil {
						redis.readTimeout = 0
					}
				case "ttl":
					if !c.NextArg() {
						return &Redis{}, c.ArgErr()
					}
					var val int
					val, err = strconv.Atoi(c.Val())
					if err != nil {
						val = defaultTtl
					}
					redis.Ttl = uint32(val)
				case "fallthrough": // 如果为空默认设置 root "."
					redis.Fall.SetZonesFromArgs(c.RemainingArgs())
				case "pgBatchUpdate":
					if !c.NextArg() {
						return &Redis{}, c.ArgErr()
					}
					var val bool
					val, err = strconv.ParseBool(c.Val())
					if err != nil {
						val = defaultBatchUpdate
					}
					redis.pgBatchUpdate = val
				default:
					if c.Val() != "}" {
						return &Redis{}, c.Errf("unknown property '%s'", c.Val())
					}
				}

				if !c.Next() {
					break
				}
			}

		}

		redis.Connect()
		redis.LoadZones()

		// 新增代码
		SharedRedisConfig = map[string]string{
			"redisPassword":  redis.redisPassword,
			"redisAddress":   redis.redisAddress,
			"keyPrefix":      redis.keyPrefix,
			"keySuffix":      redis.keySuffix,
			"connectTimeout": strconv.Itoa(redis.connectTimeout),
			"readTimeout":    strconv.Itoa(redis.readTimeout),
			"ttl":            strconv.Itoa(int(redis.Ttl)),
		}
		return &redis, nil
	}
	return &Redis{}, nil
}
