package backend

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client/models"
	json "github.com/json-iterator/go"
)

type OpentsdbBackend struct {
	enable    int
	client    *http.Client
	transport http.Transport
	interval  int
	endpoint  string
	active    bool
	running   bool
	server    string

	ch_points     chan []DataPoint
}

type DataPoint struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     interface{}       `json:"value"`
	Tags      map[string]string `json:"tags"`
}

const batchSize = 16*1024
func NewOpentsdb(config *NodeConfig) (*OpentsdbBackend, error) {
	if config.OpentsdbEnable == 0 {
		return &OpentsdbBackend{}, nil
	}

	tsdb := &OpentsdbBackend{
		client: &http.Client{
			Timeout: time.Second * time.Duration(30),
		},
		server: config.OpentsdbServer,

		interval: config.Interval,
		endpoint: config.OpentsdbServer + "/api/put",
		enable:   1,

		ch_points: make(chan []DataPoint, batchSize),
	}
	go tsdb.startLoop()
	return tsdb, nil
}

func (tsdb *OpentsdbBackend) pingChan() {
	for range time.Tick(time.Second) {
		tsdb.ch_points <- nil
	}
}

func (tsdb *OpentsdbBackend) startLoop() {
	buffer := make([]DataPoint, 2*batchSize)
	buffer = buffer[:0]
	last := time.Now()
	for data := range tsdb.ch_points {
		if data != nil {
			buffer = append(buffer, data...)
		}
		l := len(buffer)

		if l >= batchSize || time.Now().After(last.Add(time.Second*10)) {
			if l >= batchSize {
				bak := make([]DataPoint, l)
				copy(bak, buffer)
				go func() {
					if err := tsdb.send(bak); err != nil {
						// TODO retry
						log.Println("loopSend failed -", err)
					}
				}()
			} else {
				if err := tsdb.send(buffer); err != nil {
					// TODO retry
					log.Println("loopSend failed -", err)
				}
			}

			buffer = buffer[:0]
			last = time.Now()
		}
	}
}

func covertInfluxToDataPoints(p []byte) ([]DataPoint, error) {
	points, err := models.ParsePoints(p)
	if err != nil {
		return nil, err
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
	return tsdbPoints, nil
}

func (tsdb *OpentsdbBackend) Write(p []byte) error {
	if tsdb.enable == 0 {
		return nil
	}
	tsdbPoints, err := covertInfluxToDataPoints(p)
	if err != nil {
		return err
	}
	tsdb.ch_points <- tsdbPoints
	return nil
}

func (tsdb *OpentsdbBackend) send(tsdbPoints []DataPoint) error {
	data, err := json.Marshal(tsdbPoints)
	if err != nil {
		return err
	}
	//todo add compress support
	req, err := http.NewRequest("POST", tsdb.endpoint, bytes.NewReader(data))
	req.Header.Add("Content-Type", "application/json")
	resp, err := tsdb.client.Do(req)
	if err != nil {
		log.Print("http error: ", err)
		tsdb.active = false
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 204 {
		return nil
	}

	log.Print("write status code: ", resp.StatusCode)

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Print("read all error: ", err)
		return err
	}
	log.Printf("error response: %s\n", respBuf)

	// http://opentsdb.net/docs/build/html/api_http/put.html
	switch resp.StatusCode {
	case 400:
		err = ErrBadRequest
	case 404:
		err = ErrNotFound
	default: // mostly tcp connection timeout
		log.Printf("status: %d", resp.StatusCode)
		err = ErrUnknown
	}
	return err
}

func parseTags(tags string) map[string]string {
	l := strings.Split(tags, ",")
	if len(l) == 0 {
		return nil
	}
	ret := map[string]string{}
	for _, v := range l {
		ar := strings.Split(v, "=")
		if len(ar) != 2 {
			continue
		}
		ret[ar[0]] = ar[1]
	}
	return ret
}

func covertFalconToDataPoints(value []*FalconMetricValue) []DataPoint {
	var points []DataPoint
	for _, v := range value {
		x := DataPoint{
			Metric:    v.Metric,
			Timestamp: v.Timestamp,
			Value:     v.Value,
			Tags:      v.TagMap,
		}
		if _, ok := x.Tags["endpoint"]; !ok {
			x.Tags["endpoint"] = v.Endpoint
		}
		points = append(points, x)
	}
	return points
}

func (tsdb *OpentsdbBackend) WriteFalcon(value []*FalconMetricValue) error {
	if tsdb.enable == 0 {
		return nil
	}
	points := covertFalconToDataPoints(value)
	tsdb.ch_points <- points
	return nil
}

func (tsdb *OpentsdbBackend) Close() error {
	return nil
}

