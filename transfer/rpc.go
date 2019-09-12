package transfer

import (
	"github.com/shell909090/influx-proxy/backend"
	"log"
	"net"
	"net/rpc"
)

func StartRpc(addr string, ic *backend.InfluxCluster) {
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
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("listener.Accept occur error:", err)
			continue
		}

		go server.ServeCodec(NewServerCodec(conn))
	}
}
