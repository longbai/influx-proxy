package openfalcon

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/shell909090/influx-proxy/openfalcon/hbs"
	"io"
	"sort"
	"time"
)

type Agent struct {
	SddcServer string
	Hosts string
}

func (t *Agent) MinePlugins(args *AgentHeartbeatRequest, reply *AgentPluginsResponse) error {
	fmt.Println("Agent.MinePlugins")
	if args.Hostname == "" {
		return nil
	}

	reply.Plugins = []string{}
	reply.Timestamp = time.Now().Unix()
	return nil
}

func (t *Agent) ReportStatus(args *hbs.AgentReportRequest, reply *SimpleRpcResponse) (err error) {
	fmt.Println("Agent.ReportStatus")
	if args.Hostname == ""{
		uuid, err := hbs.Agents.Register(t.SddcServer, "args", nil) //t.Hosts)
		if err != nil {
			reply.Code = 1
		} else {
			reply.Code = 0
		}
		reply.UUID = uuid
		return err
	}
	uuid, err := hbs.Agents.Update(t.SddcServer, args)
	if err != nil {
		reply.Code = 2
	} else {
		reply.Code = 0
	}
	reply.UUID = uuid
	return err
}

// 需要checksum一下来减少网络开销？其实白名单通常只会有一个或者没有，无需checksum
func (t *Agent) TrustableIps(args *NullRpcRequest, ips *string) error {
	fmt.Println("Agent.TrustableIps")
	*ips = ""
	return nil
}

// agent按照server端的配置，按需采集的metric，比如net.port.listen port=22 或者 proc.num name=zabbix_agentd
func (t *Agent) BuiltinMetrics(args *AgentHeartbeatRequest, reply *BuiltinMetricResponse) error {
	fmt.Println("Agent.BuiltinMetrics")
	if args.Hostname == "" {
		return nil
	}

	metrics := []*BuiltinMetric{}

	checksum := ""
	if len(metrics) > 0 {
		checksum = digestBuiltinMetrics(metrics)
	}

	if args.Checksum == checksum {
		reply.Metrics = []*BuiltinMetric{}
	} else {
		reply.Metrics = metrics
	}
	reply.Checksum = checksum
	reply.Timestamp = time.Now().Unix()

	return nil
}

func Md5(raw string) string {
	h := md5.New()
	_, _ = io.WriteString(h, raw)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func digestBuiltinMetrics(items []*BuiltinMetric) string {
	sort.Sort(BuiltinMetricSlice(items))

	var buf bytes.Buffer
	for _, m := range items {
		buf.WriteString(m.String())
	}
	return Md5(buf.String())
}

type AgentHeartbeatRequest struct {
	Hostname string
	Checksum string
}

func (this *AgentHeartbeatRequest) String() string {
	return fmt.Sprintf(
		"<Hostname: %s, Checksum: %s>",
		this.Hostname,
		this.Checksum,
	)
}

type AgentPluginsResponse struct {
	Plugins   []string
	Timestamp int64
}

func (this *AgentPluginsResponse) String() string {
	return fmt.Sprintf(
		"<Plugins:%v, Timestamp:%v>",
		this.Plugins,
		this.Timestamp,
	)
}

// e.g. net.port.listen or proc.num
type BuiltinMetric struct {
	Metric string
	Tags   string
}

func (this *BuiltinMetric) String() string {
	return fmt.Sprintf(
		"%s/%s",
		this.Metric,
		this.Tags,
	)
}

type BuiltinMetricResponse struct {
	Metrics   []*BuiltinMetric
	Checksum  string
	Timestamp int64
}

func (this *BuiltinMetricResponse) String() string {
	return fmt.Sprintf(
		"<Metrics:%v, Checksum:%s, Timestamp:%v>",
		this.Metrics,
		this.Checksum,
		this.Timestamp,
	)
}

type BuiltinMetricSlice []*BuiltinMetric

func (this BuiltinMetricSlice) Len() int {
	return len(this)
}
func (this BuiltinMetricSlice) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}
func (this BuiltinMetricSlice) Less(i, j int) bool {
	return this[i].String() < this[j].String()
}
