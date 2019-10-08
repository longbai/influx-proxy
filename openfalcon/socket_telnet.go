package openfalcon

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

func socketTelnetHandle(conn net.Conn) {
	defer conn.Close()

	items := []*MetaData{}
	buf := bufio.NewReader(conn)

	timeout := time.Second * 30

	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		line, err := buf.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.Trim(line, "\n")

		if line == "quit" {
			break
		}

		if line == "" {
			continue
		}

		t := strings.Fields(line)
		if len(t) < 2 {
			continue
		}

		cmd := t[0]

		if cmd != "update" {
			continue
		}

		item, err := convertLine2MetaData(t[1:])
		if err != nil {
			continue
		}

		items = append(items, item)
	}

	return

}

// example: endpoint counter timestamp value [type] [step]
// default type is DERIVE, default step is 60s
func convertLine2MetaData(fields []string) (item *MetaData, err error) {
	if len(fields) != 4 && len(fields) != 5 && len(fields) != 6 {
		err = fmt.Errorf("not_enough_fileds")
		return
	}

	item = &MetaData{
		Metric:      "test",
		Endpoint:    "test",
		Timestamp:   0,
		Step:        30,
		Value:       1,
		CounterType: "GAUGE",
		Tags:        make(map[string]string),
	}

	return item, nil
}
