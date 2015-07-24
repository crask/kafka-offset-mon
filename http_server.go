package main

import (
	"encoding/json"
	"net/http"
)

type HttpServerConfig struct {
	listenAddr string

	patternLatestOffset        string
	patternConsumerGroupOffset string
}

type HttpServer struct {
	config         *HttpServerConfig
	workerRegistry map[string]*Worker
}

func NewHttpServer(config *HttpServerConfig) *HttpServer {
	if config.listenAddr == "" {
		config.listenAddr = ":8100"
	}

	if config.patternConsumerGroupOffset == "" {
		config.patternConsumerGroupOffset = "/consumer_group_offset"
	}

	if config.patternLatestOffset == "" {
		config.patternLatestOffset = "/latest_offset"
	}

	s := &HttpServer{
		config:         config,
		workerRegistry: map[string]*Worker{},
	}
	return s
}

func (this *HttpServer) Init() error {
	http.HandleFunc(this.config.patternLatestOffset, this.LatestHandler)
	http.HandleFunc(this.config.patternConsumerGroupOffset, this.ConsumerHandler)

	return nil
}

func (this *HttpServer) Start() error {
	go func() {
		http.ListenAndServe(this.config.listenAddr, nil)
	}()

	return nil
}

func (this *HttpServer) Close() error {
	return nil
}

func (this *HttpServer) getWorker(zookeeper string) (*Worker, error) {
	for k, w := range this.workerRegistry {
		if k == zookeeper {
			return w, nil
		}
	}

	if zookeeper == "" {
		zookeeper = "localhost:2181"
	}
	worker := NewWorker(zookeeper)
	err := worker.Init()
	if err != nil {
		worker = nil
	}

	return worker, err
}

func (this *HttpServer) LatestHandler(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	zookeeper := req.Form.Get("zookeeper")
	callback := req.Form.Get("callback")

	worker, err := this.getWorker(zookeeper)

	if err != nil {
		res.Write([]byte(err.Error()))
		res.WriteHeader(500)
		return
	}

	latestOffset, err := worker.GetLatestOffset()
	if err != nil {
		res.Write([]byte(err.Error()))
		res.WriteHeader(500)
		return
	}

	reponseStr, err := json.Marshal(latestOffset)
	if err != nil {
		res.Write([]byte(err.Error()))
		res.WriteHeader(500)
		return
	}

	if callback != "" {
		res.Write([]byte(callback))
	}
	res.Write(reponseStr)
}

func (this *HttpServer) ConsumerHandler(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	zookeeper := req.Form.Get("zookeeper")
	callback := req.Form.Get("callback")

	worker, err := this.getWorker(zookeeper)

	if err != nil {
		res.Write([]byte(err.Error()))
		res.WriteHeader(500)
		return
	}

	latestOffset, err := worker.GetConsumerGroupsOffset()
	if err != nil {
		res.Write([]byte(err.Error()))
		res.WriteHeader(500)
		return
	}

	reponseStr, err := json.Marshal(latestOffset)
	if err != nil {
		res.Write([]byte(err.Error()))
		res.WriteHeader(500)
		return
	}

	if callback != "" {
		res.Write([]byte(callback))
	}
	res.Write(reponseStr)
}
