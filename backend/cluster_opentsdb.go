package backend

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	json "github.com/json-iterator/go"
)

type OpentsdbBackend struct {
	enable    int
	client    *http.Client
	transport http.Transport
	interval  int
	endpoint  string
	server    string
	compress  int

	ch_points chan []*OpenTsdbDataPoint
}

type OpenTsdbDataPoint struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     json.RawMessage   `json:"value"`
	Tags      map[string]string `json:"tags"`
}

const batchSize = 16 * 1024

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
		compress: config.OpentsdbCompress,

		ch_points: make(chan []*OpenTsdbDataPoint, batchSize),
	}
	go tsdb.startLoop()
	go tsdb.pingChan()
	return tsdb, nil
}

func (tsdb *OpentsdbBackend) pingChan() {
	for range time.Tick(time.Second) {
		tsdb.ch_points <- nil
	}
}

func (tsdb *OpentsdbBackend) startLoop() {
	log.Println("opentsdb start run")
	buffer := make([]*OpenTsdbDataPoint, batchSize)
	buffer = buffer[:0]
	last := time.Now()
	for data := range tsdb.ch_points {
		if data != nil {
			buffer = append(buffer, data...)
		}
		l := len(buffer)

		if l >= batchSize || time.Now().After(last.Add(time.Second*1)) {
			if l >= batchSize {
				bak := make([]*OpenTsdbDataPoint, l)
				copy(bak, buffer)
				go func() {
					if err := tsdb.send(bak); err != nil {
						// TODO retry
						log.Println("loopSend failed more", l, err, cap(buffer), cap(tsdb.ch_points))
					}
				}()
			} else if l > 0 {
				if err := tsdb.send(buffer); err != nil {
					// TODO retry
					log.Println("loopSend failed", l, err, cap(buffer), cap(tsdb.ch_points))
				}
			}

			buffer = buffer[:0]
			last = time.Now()
		}
	}
}

func covertInfluxToDataPoints(p []byte) (tsdbPoints []*OpenTsdbDataPoint, err error) {
	lines := bytes.Split(p, []byte("\n"))

	for _, line := range lines {
		var tags = map[string]string{}
		var fields = map[string][]byte{}
		stringList := bytes.Split(line, []byte(" "))
		if len(stringList) != 3 {
			log.Println("list parse failed", len(stringList), string(line))
			continue
		}
		tagList := bytes.Split(stringList[0], []byte(","))
		measurement := tagList[0]

		for i := 1; i < len(tagList); i++ {
			kv := bytes.Split(tagList[i], []byte("="))
			if len(kv) == 2 && len(kv[1]) != 0 {
				tags[string(kv[0])] = string(kv[1])
			}
		}
		fieldList := bytes.Split(stringList[1], []byte(","))
		for _, pair := range fieldList {
			kv := bytes.Split(pair, []byte("="))
			if len(kv) == 2 && len(kv[1]) != 0 {
				fields[string(kv[0])] = kv[1]
			}
		}
		t, err := strconv.ParseInt(string(stringList[2]), 10, 64)
		if err != nil {
			log.Println("time parse failed", string(stringList[2]), t)
			continue
		}

		for k, v := range fields {
			tsdbPoints = append(tsdbPoints, &OpenTsdbDataPoint{
				Metric:    string(measurement) + "." + k,
				Timestamp: t / 1000000,
				Value:     v,
				Tags:      tags,
			})
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

func (tsdb *OpentsdbBackend) send(tsdbPoints []*OpenTsdbDataPoint) error {
	data, err := json.Marshal(tsdbPoints)
	if err != nil {
		return err
	}
	origin := data
	if tsdb.compress != 0 {
		var buf bytes.Buffer
		err := Compress(&buf, data)
		if err != nil {
			log.Printf("write file error: %s\n", err)
			return err
		}
		data = buf.Bytes()
	}

	req, err := http.NewRequest("POST", tsdb.endpoint, bytes.NewReader(data))
	req.Header.Add("Content-Type", "application/json")
	if tsdb.compress != 0 {
		req.Header.Add("Content-Encoding", "gzip")
	}
	log.Println("opentsdb sent", len(tsdbPoints))
	resp, err := tsdb.client.Do(req)
	if err != nil {
		log.Print("http error: ", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 204 {
		return nil
	}

	log.Println("write status code: ", resp.StatusCode)

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("read all error: ", err)
		return err
	}
	log.Printf("error response: %s\n", respBuf)

	// http://opentsdb.net/docs/build/html/api_http/put.html
	switch resp.StatusCode {
	case 400:
		log.Println("post failed", string(origin))
		err = ErrBadRequest
	case 404:
		err = ErrNotFound
	default: // mostly tcp connection timeout
		log.Printf("status: %d", resp.StatusCode)
		err = ErrUnknown
	}
	return err
}

func covertFalconToDataPoints(value []*FalconMetricValue) []*OpenTsdbDataPoint {
	var points []*OpenTsdbDataPoint
	for _, v := range value {
		x := &OpenTsdbDataPoint{
			Metric:    v.Metric,
			Timestamp: v.Timestamp,
			Value:     []byte(fmt.Sprint(v.Value)),
			Tags:      v.TagMap,
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
