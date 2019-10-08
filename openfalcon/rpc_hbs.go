package openfalcon

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"sort"
	"time"
)

type Agent int

var agent = Agent(0)

func (t *Agent) MinePlugins(args AgentHeartbeatRequest, reply *AgentPluginsResponse) error {
	if args.Hostname == "" {
		return nil
	}

	reply.Plugins = []string{}
	reply.Timestamp = time.Now().Unix()

	return nil
}

func (t *Agent) ReportStatus(args *AgentReportRequest, reply *SimpleRpcResponse) error {
	if args.Hostname == "" {
		reply.Code = 1
		return nil
	}
	return nil
}

// 需要checksum一下来减少网络开销？其实白名单通常只会有一个或者没有，无需checksum
func (t *Agent) TrustableIps(args *NullRpcRequest, ips *string) error {
	*ips = ""
	return nil
}

// agent按照server端的配置，按需采集的metric，比如net.port.listen port=22 或者 proc.num name=zabbix_agentd
func (t *Agent) BuiltinMetrics(args *AgentHeartbeatRequest, reply *BuiltinMetricResponse) error {
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
	io.WriteString(h, raw)
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


type AgentReportRequest struct {
	Hostname      string
	IP            string
	AgentVersion  string
	PluginVersion string
}

func (this *AgentReportRequest) String() string {
	return fmt.Sprintf(
		"<Hostname:%s, IP:%s, AgentVersion:%s, PluginVersion:%s>",
		this.Hostname,
		this.IP,
		this.AgentVersion,
		this.PluginVersion,
	)
}

type AgentUpdateInfo struct {
	LastUpdate    int64
	ReportRequest *AgentReportRequest
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

