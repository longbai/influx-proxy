package transfer

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

type NullRpcRequest struct {
}

// code == 0 => success
// code == 1 => bad request
type SimpleRpcResponse struct {
	Code int `json:"code"`
}

func (this *SimpleRpcResponse) String() string {
	return fmt.Sprintf("<Code: %d>", this.Code)
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
	reply.Latency = (time.Now().UnixNano() - start.UnixNano()) / 1000000
	return nil
}