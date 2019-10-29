package openfalcon

import (
	"fmt"
	"log"
	"net"
	"net/rpc"

	"github.com/shell909090/influx-proxy/backend"
)

func StartRpc(addr string, ic *backend.InfluxCluster, sddcServer, hosts string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatalf("net.ResolveTCPAddr fail: %s", err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatalf("listen %s fail: %s", addr, err)
	} else {
		log.Println("rpc listening", addr)
	}

	server := rpc.NewServer()
	err = server.Register(&Transfer{ic})
	if err != nil {
		log.Fatalf("regisger fail: %s", err)
	}
	//err = server.Register(&Agent{SddcServer:sddcServer, Hosts:hosts})
	//if err != nil {
	//	log.Fatalf("regisger fail: %s", err)
	//}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("listener.Accept occur error:", err)
			continue
		}

		go server.ServeCodec(NewServerCodec(conn))
	}
}

type NullRpcRequest struct {
}

// code == 0 => success
// code == 1 => bad request
type SimpleRpcResponse struct {
	Code int `json:"code"`
	UUID string `json:"uuid,omitempty"`
}

func (this *SimpleRpcResponse) String() string {
	return fmt.Sprintf("<Code: %d>", this.Code)
}
