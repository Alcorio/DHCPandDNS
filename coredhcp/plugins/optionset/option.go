package optionset

import (
    "encoding/hex"
    "errors"
    "fmt"
    "net"
    "strings"

    "github.com/coredhcp/coredhcp/handler"
    "github.com/coredhcp/coredhcp/logger"
    "github.com/coredhcp/coredhcp/plugins"
    "github.com/insomniacslk/dhcp/dhcpv4"
    "github.com/insomniacslk/dhcp/dhcpv6"
)

var log = logger.GetLogger("plugins/optionset")

// Plugin 插件注册信息
var Plugin = plugins.Plugin{
    Name:   "optionset",
    Setup4: setup4, // DHCPv4
    Setup6: setup6, // DHCPv6
}

// 全局变量用来存储解析到的参数
var (
    globalOption60 string   // Option60 (Vendor Class Identifier)
    globalOption43 []byte   // Option43 (Vendor Specific Info)
    useVendor      string   // 指定是 "huawei" 还是 "cisco"
    acIP           string   // 从配置获取的 AC IP
)

// setup4 解析配置文件中传进来的参数
// 例如：
//   plugins:
//     - optionset: "vendor=huawei" "ac_ip=192.168.100.1"
// 或
//   plugins:
//     - optionset: "option60=MyVendorClass" "option43=0104c0a86401"
func setup4(args ...string) (handler.Handler4, error) {
    // 每次初始化时都清空
    globalOption60 = ""
    globalOption43 = nil
    useVendor = ""
    acIP = ""

    for _, arg := range args {
        // 1) 解析 vendor
        if strings.HasPrefix(arg, "vendor=") {
            useVendor = strings.ToLower(strings.TrimPrefix(arg, "vendor="))
            log.Infof("Parsed vendor=%s", useVendor)
        }

        // 2) 解析 ac_ip
        if strings.HasPrefix(arg, "ac_ip=") {
            acIP = strings.TrimPrefix(arg, "ac_ip=")
            log.Infof("Parsed ac_ip=%s", acIP)
        }

        // 3) 解析 option43 (手动指定)
        if strings.HasPrefix(arg, "option43=") {
            hexStr := strings.TrimPrefix(arg, "option43=")
            decoded, err := hex.DecodeString(hexStr)
            if err != nil {
                return nil, fmt.Errorf("failed to decode option43 hex string %q: %w", hexStr, err)
            }
            globalOption43 = decoded
            log.Infof("Parsed custom option43=%X", globalOption43)
        }

        // 4) 解析 option60
        if strings.HasPrefix(arg, "option60=") {
            globalOption60 = strings.TrimPrefix(arg, "option60=")
            log.Infof("Parsed option60=%s", globalOption60)
        }
    }

    // 如果用户没有手动指定 option43，而又指定了 vendor & ac_ip，则自动生成
    if len(globalOption43) == 0 && useVendor != "" && acIP != "" {
        // 根据 vendor + ac_ip 生成对应的 option43
        generated, err := generateOption43(useVendor, acIP)
        if err != nil {
            return nil, fmt.Errorf("failed to generate option43 for vendor=%s ac_ip=%s: %w", useVendor, acIP, err)
        }
        globalOption43 = generated
        log.Infof("Automatically generated option43 for %s: %X", useVendor, globalOption43)
    }

    // 返回我们的 handler4
    return handler4, nil
}

// setup6 暂时不处理 DHCPv6 (option 43/60 主要在 DHCPv4 常用)，这里直接透传
func setup6(args ...string) (handler.Handler6, error) {
    if len(args) > 0 {
        log.Warningf("optionset plugin: DHCPv6 environment does not normally use option43/60, ignoring args=%v", args)
    }
    return handler6, nil
}

// handler4 在 DHCPv4 报文中写入或覆盖 option60 与 option43
func handler4(req, resp *dhcpv4.DHCPv4) (*dhcpv4.DHCPv4, bool) {
    // 如果配置文件设置了 option60，更新 Vendor Class Identifier (code 60)
    if globalOption60 != "" {
        resp.UpdateOption(dhcpv4.OptClassIdentifier(globalOption60))
        log.Debugf("Set DHCPv4 option 60 to: %s", globalOption60)
    }

    // 如果有计算/解析到的 option43，则更新 Vendor Specific Information (code 43)
    if len(globalOption43) > 0 {
        resp.UpdateOption(dhcpv4.Option{
            Code:  dhcpv4.OptionVendorSpecificInformation,
            Value: GenericOptionValue(globalOption43),
        })
        log.Debugf("Set DHCPv4 option 43 to hex: %X", globalOption43)
    }

    return resp, false
}

// handler6 暂时原样返回
func handler6(req, resp dhcpv6.DHCPv6) (dhcpv6.DHCPv6, bool) {
    return resp, false
}

// generateOption43 根据厂商类型和 AC IP 生成华为或思科对应的 option43
//   - 华为: 01 + 04*N + <IP列表>
//   - 思科: f1 + 04*N + <IP列表>
// 这里仅做单个 IP 示例，若需多个 IP 可自行扩展。
func generateOption43(vendor string, ipStr string) ([]byte, error) {
    parsedIP := net.ParseIP(ipStr).To4()
    if parsedIP == nil {
        return nil, fmt.Errorf("invalid IPv4 address %s", ipStr)
    }

    // 将 IP 转化为 4 bytes
    ipHex := parsedIP

    // 长度 = 4 * IP个数，这里假设只有 1 个 IP
    length := byte(4)

    switch vendor {
    case "huawei":
        // 01 + 04 + ipHex
        // 01 表示类型
        // 04 表示后面内容长度
        // c0 a8 64 01 为 IP 的十六进制
        result := []byte{0x01, length}
        result = append(result, ipHex...)
        return result, nil

    case "cisco":
        // f1 + 04 + ipHex
        // f1 表示类型 (思科固定)
        // 04 表示后面内容长度
        result := []byte{0xf1, length}
        result = append(result, ipHex...)
        return result, nil
        
    default:
        return nil, errors.New("unknown vendor: " + vendor)
    }
}

// 定义一个通用的 OptionValue，用于装载任意 []byte
type GenericOptionValue []byte

func (g GenericOptionValue) ToBytes() []byte {
    return g
}

func (g GenericOptionValue) String() string {
    return fmt.Sprintf("%X", []byte(g))
}