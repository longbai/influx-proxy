package backend

import (
	"bytes"
	"net/http"
	"time"

	"github.com/influxdata/influxdb1-client/models"
	json "github.com/json-iterator/go"
)

type OpentsdbBackend struct {
	enable   int
	endpoint string
}

type DataPoint struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     interface{}       `json:"value"`
	Tags      map[string]string `json:"tags"`
}

func NewOpentsdb(config *NodeConfig) (*OpentsdbBackend, error) {
	if config.OpentsdbEnable == 0 {
		return &OpentsdbBackend{}, nil
	}
	url := config.OpentsdbServer + "/api/put"
	return &OpentsdbBackend{
		enable:   1,
		endpoint: url,
	}, nil
}

func (tsdb *OpentsdbBackend) Write(p []byte) error {
	if tsdb.enable == 0 {
		return nil
	}
	points, err := models.ParsePointsWithPrecision(p, time.Now(), "ns")
	if err != nil {
		return err
	}
	var tsdbPoints []DataPoint
	for _, v := range points {
		tags := v.Tags().Map()
		iter := v.FieldIterator()
		for ; ; {
			var value interface{}
			switch iter.Type() {
			case models.Integer:
				value, _ = iter.IntegerValue()
			case models.Float:
				value, _ = iter.FloatValue()
			case models.String:
				value = iter.StringValue()
			}
			dp := DataPoint{
				Metric:    string(v.Name()) + "." + string(iter.FieldKey()),
				Timestamp: v.Time().UnixNano() / 1000000,
				Value:     value,
				Tags:      tags,
			}
			tsdbPoints = append(tsdbPoints, dp)
		}
	}
	data, err := json.Marshal(tsdbPoints)
	if err != nil {
		return err
	}
	_, err = http.Post(tsdb.endpoint, "application/json", bytes.NewReader(data))
	return err
}

func (tsdb *OpentsdbBackend) Close() error {
	return nil
}
