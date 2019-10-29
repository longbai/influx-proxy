package hbs

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

func Init() {

}

type SafeAgents struct {
	sync.RWMutex
	M map[string]*AgentUpdateInfo
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

var Agents = &SafeAgents{M: make(map[string]*AgentUpdateInfo)}

func UpdateAgent(info *AgentUpdateInfo, hosts string) {
	sql := ""
	//判断hosts 是sync（cmdb 同步到hosts) 还是为空（agent上报）
	if hosts == "" {
		sql = fmt.Sprintf(
			"insert into vm_tb(vm_name,vm_ip,agent_version,plugin_version,vm_uuid) values('%s','%s','%s','%s','%s') on duplicate key upate vm_name='%s',agent_version='%s',plugin_version='%s',vm_uuid='%s'",
			info.ReportRequest.IP,
			info.ReportRequest.IP,
			info.ReportRequest.AgentVersion,
			info.ReportRequest.PluginVersion,
			info.ReportRequest.Hostname,
			info.ReportRequest.IP,
			info.ReportRequest.AgentVersion,
			info.ReportRequest.PluginVersion,
			info.ReportRequest.Hostname,
			)
	} else {
		sql = fmt.Sprintf(
			"update vm_tb set vm_name='%s', agent_version='%s', plugin_version='%s', vm_uuid='%s' where vm_ip='%s'",
			info.ReportRequest.Hostname,
			info.ReportRequest.AgentVersion,
			info.ReportRequest.PluginVersion,
			info.ReportRequest.Hostname,
			info.ReportRequest.IP,
			)
	}
	_, err := db.Exec(sql)
	if err != nil {
		log.Println("db exec failed", err)
	}
}

func (this *SafeAgents) Register(sddcServer, hosts string, req *AgentReportRequest) (uuid string, err error) {
	var val = &AgentUpdateInfo{
		LastUpdate:    time.Now().Unix(),
		ReportRequest: req,
	}

	uuid, exist := HostMap.GetUUID(req.IP)
	if !exist || uuid == "" {
		if sddcServer != "" {
			uuid, err = SDDCRegister(sddcServer, req.IP)
		} else {
			uuid, err = LocalRegister()
		}
		if err != nil {
			return uuid, err
		}
		val.ReportRequest.Hostname = uuid

		UpdateAgent(val, hosts)

		err = HostMap.Register(uuid, req.IP)
		if err != nil {
			return uuid, err
		}
	} else {
		//
	}
	this.Lock()
	this.M[req.Hostname] = val
	this.Unlock()
	return uuid, err

}

func (this *SafeAgents) Update(sddc_server string, req *AgentReportRequest) (uuid string, err error) {
	var val = &AgentUpdateInfo{
		LastUpdate:    time.Now().Unix(),
		ReportRequest: req,
	}
	uuid, exist := HostMap.GetUUID(req.IP)
	if !exist {
		msg := fmt.Sprintf("host %s does not exist in cache.", req.IP)
		err = errors.New(msg)
		return uuid, err
	}
	if uuid != req.Hostname {
		msg := fmt.Sprintf("host %s 's uuid(%s) in cache does not match agent's uuid(%s)", req.IP, uuid, req.Hostname)
		err = errors.New(msg)
		return uuid, err
	}
	//db.UpdateAgent(val)
	this.Lock()
	this.M[req.Hostname] = val
	this.Unlock()
	return uuid, err
}

type Result struct {
	Vm_ip      string `json:"vm_ip"`
	Reg_id     string `json:"reg_id"`
	ResultCode int    `json:"resultCode"`
	ResultDesc string `json:"resultDesc"`
}

func LocalRegister() (string, error) {
	return getGuid()
}

func SDDCRegister(sddcServer, ip string) (uuid string, err error) {
	result := &Result{}
	resturl := sddcServer + "?vm_ip=" + ip
	client := &http.Client{}
	request, err := http.NewRequest("GET", resturl, nil)
	if err != nil {
		return uuid, err
	}
	request.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(request)
	if err != nil {
		return uuid, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return uuid, errors.New("call sddc restfull api return statuscode is not 200")
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return uuid, err
	}
	err = json.Unmarshal(body, result)
	if err != nil {
		return uuid, err
	}
	if result.ResultCode != 200 {
		return uuid, errors.New(result.ResultDesc)
	}
	return result.Reg_id, nil
}

func getMd5String(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func getGuid() (string, error) {
	b := make([]byte, 48)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return getMd5String(base64.URLEncoding.EncodeToString(b)), nil
}
