package openfalcon

import (
	"fmt"
	"github.com/shell909090/influx-proxy/backend"
	"time"
)

type Transfer struct {
	Ic *backend.InfluxCluster
}

type TransferResp struct {
	Msg        string
	Total      int
	ErrInvalid int
	Latency    int64
}


func (t *TransferResp) String() string {
	s := fmt.Sprintf("TransferResp total=%d, err_invalid=%d, latency=%dms",
		t.Total, t.ErrInvalid, t.Latency)
	if t.Msg != "" {
		s = fmt.Sprintf("%s, msg=%s", s, t.Msg)
	}
	return s
}

func (this *Transfer) Ping(req NullRpcRequest, resp *SimpleRpcResponse) error {
	fmt.Println("transfer ping")
	return nil
}

type TransferResponse struct {
	Message string
	Total   int
	Invalid int
	Latency int64
}

func (this *TransferResponse) String() string {
	return fmt.Sprintf(
		"<Total=%v, Invalid:%v, Latency=%vms, Message:%s>",
		this.Total,
		this.Invalid,
		this.Latency,
		this.Message,
	)
}

func (t *Transfer) Update(args []*backend.FalconMetricValue, reply *TransferResponse) error {
	start := time.Now()
	reply.Invalid = 0
	_, _ = t.Ic.FalconPushMetric(args)
	reply.Message = "ok"
	reply.Total = len(args)
	tt := time.Now().UnixNano() - start.UnixNano()
	reply.Latency = tt / 1000000
	fmt.Println("Transfer.Update", len(args), "Duration", tt/1000, "μs")
	return nil
}
