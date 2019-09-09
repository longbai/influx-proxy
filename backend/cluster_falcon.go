package backend

import (
	"bytes"
	"fmt"
	"log"
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
	Timestamp int64       `json:"timestamp"`
}

func convertFalconToInfluxBytes(metrics *FalconMetricValue) []byte {
	s := fmt.Sprintf("%s,%s value=%v %d", metrics.Metric, metrics.Tags, metrics.Value, metrics.Timestamp)
	return []byte(s)
}

func (ic *InfluxCluster) FalconPush(p []byte) (count int, err error) {
	var metrics []*FalconMetricValue
	err = json.Unmarshal(p, &metrics)

	atomic.AddInt64(&ic.stats.WriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ic.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	var lines [][]byte
	for _, v := range metrics  {
		v.TagMap = parseTags(v.Tags)
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
