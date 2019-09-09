// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	redis "gopkg.in/redis.v5"
	"github.com/gin-gonic/gin"

	"github.com/shell909090/influx-proxy/backend"
)

var (
	ErrConfig   = errors.New("config parse error")
	ConfigFile  string
	NodeName    string
	RedisAddr   string
	LogFilePath string

	OpentsdbAddr 	string
	KafkaAddr 		string
	KafkaTopic 		string
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	flag.StringVar(&LogFilePath, "log-file-path", "/var/log/influx-proxy.log", "output file")
	flag.StringVar(&ConfigFile, "config", "", "config file")
	flag.StringVar(&NodeName, "node", "l1", "node name")
	flag.StringVar(&RedisAddr, "redis", "localhost:6379", "redis server")

	flag.StringVar(&OpentsdbAddr, "tsdb", "", "open tsdb server")

	flag.StringVar(&KafkaAddr, "kafka", "", "kafka server")
	flag.StringVar(&KafkaTopic, "topic", "", "kafka topic")
	flag.Parse()
}

type Config struct {
	redis.Options
	Node string
}

func LoadJson(configfile string, cfg interface{}) (err error) {
	file, err := os.Open(configfile)
	if err != nil {
		return
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	err = dec.Decode(&cfg)
	return
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
	var cfg Config

	if ConfigFile != "" {
		err = LoadJson(ConfigFile, &cfg)
		if err != nil {
			log.Print("load config failed: ", err)
			return
		}
		log.Printf("json loaded.")
	}

	if NodeName != "" {
		cfg.Node = NodeName
	}

	if RedisAddr != "" {
		cfg.Addr = RedisAddr
	}

	rcs := backend.NewRedisConfigSource(&cfg.Options, cfg.Node)

	nodecfg, err := rcs.LoadNode()
	if err != nil {
		log.Printf("config source load failed.")
		return
	}

	if KafkaAddr != "" && KafkaTopic != ""{
		nodecfg.KafkaTopic = KafkaTopic
		nodecfg.KafkaServers = KafkaAddr
		nodecfg.KafkaEnable = 1
	}

	if OpentsdbAddr != ""{
		 nodecfg.OpentsdbServer = OpentsdbAddr
		 nodecfg.OpentsdbEnable = 1
	}

	ic := backend.NewInfluxCluster(rcs, &nodecfg)
	ic.LoadConfig()

	mux := gin.Default()
	NewHttpService(ic, nodecfg.DB).Register(mux)

	log.Printf("http service start.")

	server := &http.Server{
		Addr:        nodecfg.ListenAddr,
		Handler:     mux,
		IdleTimeout: time.Duration(nodecfg.IdleTimeout) * time.Second,
	}
	if nodecfg.IdleTimeout <= 0 {
		server.IdleTimeout = 10 * time.Second
	}
	err = server.ListenAndServe()
	if err != nil {
		log.Print(err)
		return
	}
}
