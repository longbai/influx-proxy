package hbs

import "sync"

type SafeHostMap struct {
	sync.RWMutex
	M   map[string]int
	Ips map[string]string
	N   map[int]string
	D   map[int]string
	Reg map[string]string
}

var HostMap = &SafeHostMap{
	M:   make(map[string]int),
	N:   make(map[int]string),
	Ips: make(map[string]string),
	D:   make(map[int]string),
	Reg: make(map[string]string),
}

func (this *SafeHostMap) GetUUID(ip string) (s string, b bool) {
	this.RLock()
	s, b = this.Reg[ip]
	this.RUnlock()
	return
}

func (this *SafeHostMap) Register(uuid, ip string) (err error) {
	return
}
