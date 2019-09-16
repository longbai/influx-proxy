// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shell909090/influx-proxy/backend"
	"github.com/shell909090/influx-proxy/transfer"
	"gopkg.in/natefinch/lumberjack.v2"

	_ "net/http/pprof"
)

var (
	ErrConfig   = errors.New("config parse error")
	ConfigFile  string
	NodeName    string
	RedisConf   string
	LogFilePath string
	TransferAddr string

	OpentsdbAddr string
	KafkaAddr    string
	KafkaTopic   string
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	flag.StringVar(&LogFilePath, "log-file-path", "/var/log/influx-proxy.log", "output file")
	flag.StringVar(&ConfigFile, "config", "", "config file")
	flag.StringVar(&NodeName, "node", "l1", "node name")
	flag.StringVar(&RedisConf, "redis-conf", "redis.conf", "redis server config file")
	flag.StringVar(&TransferAddr, "transfer-bind", ":8433", "transfer server addr")
	flag.Parse()
}

func initLog() {
	if LogFilePath == "" {
		log.SetOutput(os.Stdout)
	} else {
		log.SetOutput(&lumberjack.Logger{
			Filename:   LogFilePath,
			MaxSize:    100,
			MaxBackups: 5,
			MaxAge:     7,
		})
	}
}

func main() {
	initLog()

	var err error
	var configSource backend.ConfigSource
	if ConfigFile != "" {
		configSource, err = backend.NewFileConfigSource(ConfigFile)
		if err != nil {
			log.Print("load config failed: ", err)
			return
		}
		log.Printf("file config loaded.")
	} else if RedisConf != "" {
		configSource, err = backend.NewRedisConfigSourceFromFile(RedisConf, NodeName)
		if err != nil {
			log.Print("load config failed: ", err)
			return
		}
		log.Printf("file config loaded.")
	}

	nodeCfg, err := configSource.LoadNode()
	if err != nil {
		log.Printf("config source load failed.")
		return
	}

	ic := backend.NewInfluxCluster(configSource, &nodeCfg)
	ic.LoadConfig()

	mux := gin.Default()
	NewHttpService(ic, nodeCfg.DB).Register(mux)

	log.Printf("http service start.")

	server := &http.Server{
		Addr:        nodeCfg.ListenAddr,
		Handler:     mux,
		IdleTimeout: time.Duration(nodeCfg.IdleTimeout) * time.Second,
	}
	if nodeCfg.IdleTimeout <= 0 {
		server.IdleTimeout = 10 * time.Second
	}
	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()

	go transfer.StartRpc(TransferAddr, ic)
	err = server.ListenAndServe()
	if err != nil {
		log.Print(err)
		return
	}
}
