package backend

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	json "github.com/json-iterator/go"
)

type FalconMetricValue struct {
	Endpoint  string      `json:"endpoint"`
	Metric    string      `json:"metric"`
	Value     interface{} `json:"value"`
	Step      int64       `json:"step"`
	Type      string      `json:"counterType"`
	Tags      string      `json:"tags"`
	TagMap    map[string]string
	Timestamp int64 `json:"timestamp"`
}

func convertFalconToInfluxBytes(metrics *FalconMetricValue) []byte {
	b := strings.Builder{}
	b.WriteString(metrics.Metric)
	for k, v := range metrics.TagMap {
		b.WriteString(",")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(v)
	}
	b.WriteString(" ")
	b.WriteString("value=")
	b.WriteString(fmt.Sprint(metrics.Value))
	b.WriteString(" ")
	b.WriteString(strconv.FormatInt(metrics.Timestamp*1000000000, 10))
	return []byte(b.String())
}

func parseTags(tags string) map[string]string {
	tags = strings.Replace(tags, " ", "", -1)
	l := strings.Split(tags, ",")
	if len(l) == 0 {
		return nil
	}
	ret := map[string]string{}
	for _, v := range l {
		ar := strings.Split(v, "=")
		if len(ar) != 2 || ar[1] == "" {
			continue
		}
		ret[ar[0]] = ar[1]
	}
	return ret
}

func (ic *InfluxCluster) FalconPushMetric(metrics []*FalconMetricValue) (count int, err error) {
	atomic.AddInt64(&ic.stats.WriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ic.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	var lines [][]byte
	for _, v := range metrics {
		v.TagMap = parseTags(v.Tags)
		if v.Endpoint != "" {
			if _, ok := v.TagMap["endpoint"]; !ok {
				v.TagMap["endpoint"] = v.Endpoint
			}
		}
		data := convertFalconToInfluxBytes(v)
		lines = append(lines, data)
		_ = ic.WriteRow(data)
	}
	_ = ic.opentsdb.WriteFalcon(metrics)
	ic.lock.RLock()
	defer ic.lock.RUnlock()
	if len(ic.bas) > 0 {
		p := bytes.Join(lines, []byte("\n"))
		for _, n := range ic.bas {
			err = n.Write(p)
			if err != nil {
				log.Printf("error: %s\n", err)
				atomic.AddInt64(&ic.stats.WriteRequestsFail, 1)
			}
		}
	}
	return len(metrics), err
}

func (ic *InfluxCluster) FalconPush(r io.Reader) (count int, err error) {
	var metrics []*FalconMetricValue
	d := json.NewDecoder(r)
	err = d.Decode(&metrics)
	if err != nil {
		return 0, err
	}
	return ic.FalconPushMetric(metrics)
}
