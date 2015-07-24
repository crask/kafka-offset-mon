package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	httpsvrConfig := &HttpServerConfig{}
	httpsvr := NewHttpServer(httpsvrConfig)
	httpsvr.Init()
	httpsvr.Start()

	influxdbSyncerConfig := &InfluxdbSyncerConfig{}
	influxdbSyncer := NewInfluxdbSyncer(influxdbSyncerConfig)
	influxdbSyncer.Init()
	influxdbSyncer.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-c:
		log.Printf("catch exit signal")
		httpsvr.Close()
		influxdbSyncer.Close()
		log.Printf("exit done")
	}
}
