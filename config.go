package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	HttpServer      HttpServerConfig       `json:"http_server"`
	InfluxdbSyncers []InfluxdbSyncerConfig `json:"influxdbSyncers"`
}

func loadConfig(configFile string) (*Config, error) {
	var c *Config
	path := configFile
	fi, err := os.Open(path)
	defer fi.Close()
	if nil != err {
		return nil, err
	}

	fd, err := ioutil.ReadAll(fi)

	err = json.Unmarshal([]byte(fd), &c)
	if nil != err {
		return nil, err
	}
	log.Printf("%#v", c)
	return c, nil
}
