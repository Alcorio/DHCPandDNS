package coredns_postgresql

import(
	"testing"
	// "fmt"
	"log"
	"encoding/json"
)

// 直接点击run test看不到fmt的信息，再终端输入如下
// go test -v -run ^TestJson$ github.com/coredns/coredns/plugin/coredns_postgresql
func TestJson(t *testing.T) {
	type SOAValue struct {
		MBox string `json:"mBox"`
	}
	
	recordContent := `{"Mbox": "example@example.com"}`
	var soaValue SOAValue
	err := json.Unmarshal([]byte(recordContent), &soaValue)
	if err != nil {
		panic(err)
	}
	log.Printf("%v", soaValue.MBox)
	// fmt.Println(soaValue.MBox) // 输出: ""
}