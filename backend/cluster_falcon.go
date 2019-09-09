package backend

import (
	json "github.com/json-iterator/go"
)

type FalconMetricValue struct {
	Endpoint  string      `json:"endpoint"`
	Metric    string      `json:"metric"`
	Value     interface{} `json:"value"`
	Step      int64       `json:"step"`
	Type      string      `json:"counterType"`
	Tags      string      `json:"tags"`
	Timestamp int64       `json:"timestamp"`
}

func (ic *InfluxCluster) FalconPush(p []byte) (err error) {
	var metrics []*FalconMetricValue
	err = json.Unmarshal(p, &metrics)
	return
}
