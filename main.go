package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	configFile string
	version    bool
	testMode   bool
)

func init() {
	flag.StringVar(&configFile, "c", "config.json", "the config file")
	flag.BoolVar(&version, "V", false, "show version")
	flag.BoolVar(&testMode, "t", false, "test config")
}

func main() {

	flag.Parse()

	if version {
		//showVersion()
		return
	}

	if testMode {
		fmt.Println("config test ok")
		return
	}

	config, err := loadConfig(configFile)
	if err != nil {
		log.Fatalf("Config load error:%s", err.Error())
		os.Exit(-1)
	}

	sm := &ServerManager{}

	if config.HttpServer.ListenAddr != "" {
		sm.AddHttpServer(NewHttpServer(&config.HttpServer))
	} else {
		log.Printf("No httpserver config found")
	}

	if len(config.InfluxdbSyncers) > 0 {
		for _, c := range config.InfluxdbSyncers {
			sm.AddInfluxdbSyncer(NewInfluxdbSyncer(&c))
		}
	} else {
		log.Printf("No influxdbsyncer config found")
	}

	err = sm.Init()
	if err != nil {
		log.Fatalf("Init servers failed:%s", err.Error())
		os.Exit(-1)
	}

	err = sm.Start()
	if err != nil {
		log.Fatalf("Start servers failed:%s", err.Error())
		os.Exit(-1)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-c:
		log.Printf("catch exit signal")
		sm.Close()
		log.Printf("exit done")
	}
}
