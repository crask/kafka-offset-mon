package main

import (
	"encoding/json"
	"net/http"
)

type HttpServerConfig struct {
	ListenAddr                   string `json:"listenAddr"`
	PatternLatestOffset          string `json:"patternLatestOffset"`
	PatternConsumerGroupOffset   string `json:"patternConsumerGroupOffset"`
	PatternConsumerGroupDistance string `json:"patternConsumerGroupDistance"`
}

type HttpServer struct {
	config         *HttpServerConfig
	workerRegistry map[string]*Worker
}

func NewHttpServer(config *HttpServerConfig) *HttpServer {
	if config.ListenAddr == "" {
		config.ListenAddr = ":8100"
	}

	if config.PatternConsumerGroupOffset == "" {
		config.PatternConsumerGroupOffset = "/consumer_group_offset"
	}

	if config.PatternConsumerGroupDistance == "" {
		config.PatternConsumerGroupDistance = "/consumer_group_distance"
	}

	if config.PatternLatestOffset == "" {
		config.PatternLatestOffset = "/latest_offset"
	}

	s := &HttpServer{
		config:         config,
		workerRegistry: map[string]*Worker{},
	}
	return s
}

func (this *HttpServer) Init() error {
	http.HandleFunc(this.config.PatternLatestOffset, this.LatestOffsetHandler)
	http.HandleFunc(this.config.PatternConsumerGroupOffset, this.ConsumerGroupOffsetHandler)
	http.HandleFunc(this.config.PatternConsumerGroupDistance, this.ConsumerGroupDistanceHandler)

	return nil
}

func (this *HttpServer) Start() error {
	go func() {
		http.ListenAndServe(this.config.ListenAddr, nil)
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

func (this *HttpServer) LatestOffsetHandler(res http.ResponseWriter, req *http.Request) {
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

func (this *HttpServer) ConsumerGroupOffsetHandler(res http.ResponseWriter, req *http.Request) {
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

func (this *HttpServer) ConsumerGroupDistanceHandler(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	zookeeper := req.Form.Get("zookeeper")
	callback := req.Form.Get("callback")

	worker, err := this.getWorker(zookeeper)

	if err != nil {
		res.Write([]byte(err.Error()))
		res.WriteHeader(500)
		return
	}

	latestOffset, err := worker.GetConsumerGroupsOffsetDistance()
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
