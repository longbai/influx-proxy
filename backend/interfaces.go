// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import "net/http"

type Querier interface {
	Query(w http.ResponseWriter, req *http.Request) (err error)
}

type BackendAPI interface {
	Querier
	IsActive() (b bool)
	IsWriteOnly() (b bool)
	Ping() (version string, err error)
	GetZone() (zone string)
	Write(p []byte) (err error)
	Close() (err error)
}

type NodeConfig struct {
	ListenAddr   string `yaml:"addr"`
	DB           string `yaml:"db"`
	Zone         string `yaml:"zone"`
	Nexts        string `yaml:"nexts"`
	Interval     int    `yaml:"interval"`
	IdleTimeout  int    `yaml:"idle_timeout"`
	WriteTracing int    `yaml:"write_tracing"`
	QueryTracing int    `yaml:"query_tracing"`

	KafkaEnable       int    `yaml:"kafka"`
	KafkaTopic        string `yaml:"kafka_topic"`
	KafkaServers      string `yaml:"kafka_servers"`
	KafkaCompressMode string `yaml:"kafka_compress_mode"`

	OpentsdbEnable   int    `yaml:"opentsdb"`
	OpentsdbServer   string `yaml:"opentsdb_server"`
	OpentsdbCompress int    `yaml:"opentsdb_compress"`
}

type BackendConfig struct {
	URL             string `yaml:"url"`
	DB              string `yaml:"db"`
	Zone            string `yaml:"zone"`
	Interval        int    `yaml:"interval"`
	Timeout         int    `yaml:"timeout"`
	TimeoutQuery    int    `yaml:"timeout_query"`
	MaxRowLimit     int    `yaml:"max_row_limit"`
	CheckInterval   int    `yaml:"check_interval"`
	RewriteInterval int    `yaml:"rewrite_interval"`
	WriteOnly       int    `yaml:"write_only"`
}

type ConfigSource interface {
	LoadNode() (NodeConfig, error)
	LoadAllBackends() (map[string]*BackendConfig, error)
	LoadBackend(string) (*BackendConfig, error)
	LoadMeasurements() (map[string][]string, error)
}
