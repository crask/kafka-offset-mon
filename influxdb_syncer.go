package main

import (
	"errors"
	"log"
	"net/url"
	"time"

	"github.com/influxdb/influxdb/client"
)

type InfluxdbSyncerConfig struct {
	Zookeeper                              string `json:"zookeeper"`
	InfluxdbHost                           string `json:"influxdbHost"`
	InfluxdbUser                           string `json:"influxdbUser"`
	InfluxdbPassword                       string `json:"influxdbPassword"`
	InfluxdbDb                             string `json:"influxdbDb"`
	InfluxdbRetentionPolicy                string `json:"influxdbRetentionPolicy"`
	InfluxdbMeasurementLatestOffset        string `json:"influxdbMeasurementLatestOffset"`
	InfluxdbMeasurementConsumerGroupOffset string `json:"influxdbMeasurementConsumerGroupOffset"`
	Interval                               string `json:"interval"`
}

type InfluxdbSyncer struct {
	config   *InfluxdbSyncerConfig
	worker   *Worker
	dbclient *client.Client
	ticker   *time.Ticker
}

func NewInfluxdbSyncer(config *InfluxdbSyncerConfig) *InfluxdbSyncer {
	if config.Zookeeper == "" {
		config.Zookeeper = "127.0.0.1:2181"
	}
	if config.InfluxdbHost == "" {
		config.InfluxdbHost = "http://127.0.0.1:8086"
	}
	if config.InfluxdbUser == "" {
		config.InfluxdbUser = "root"
	}
	if config.InfluxdbPassword == "" {
		config.InfluxdbPassword = "root"
	}
	if config.InfluxdbDb == "" {
		config.InfluxdbDb = "kafka_monitor"
	}
	if config.InfluxdbRetentionPolicy == "" {
		config.InfluxdbRetentionPolicy = "default"
	}
	if config.InfluxdbMeasurementConsumerGroupOffset == "" {
		config.InfluxdbMeasurementConsumerGroupOffset = "consumer_group_offset"
	}
	if config.InfluxdbMeasurementLatestOffset == "" {
		config.InfluxdbMeasurementLatestOffset = "latest_offset"
	}

	if config.Interval == "" {
		config.Interval = "5s"
	}

	s := &InfluxdbSyncer{config: config}
	return s
}

func (this *InfluxdbSyncer) Init() error {

	/* init worker */
	worker := NewWorker(this.config.Zookeeper)
	err := worker.Init()
	if err != nil {
		return err
	}

	this.worker = worker

	/* init influxdb */
	influxdbUrl, err := url.Parse(this.config.InfluxdbHost)
	if err != nil {
		return err
	}

	conf := client.Config{
		URL:       *influxdbUrl,
		Username:  this.config.InfluxdbUser,
		Password:  this.config.InfluxdbPassword,
		UserAgent: "kafka-offset-mon",
	}
	con, err := client.NewClient(conf)
	if err != nil {
		return err
	}

	this.dbclient = con

	/* init ticker */

	duration, err := time.ParseDuration(this.config.Interval)
	if err != nil {
		duration = time.Second * 5
	}
	this.ticker = time.NewTicker(duration)

	return nil
}

func (this *InfluxdbSyncer) Start() error {

	if this.worker == nil || this.dbclient == nil || this.ticker == nil {
		return errors.New("not init")
	}

	go func() {
		for {
			select {

			case <-this.ticker.C:
				log.Printf("[InfluxdbSyncer]start sync for %s", this.config.Zookeeper)
				err := this.syncLatestOffset()
				if err != nil {
					log.Printf("[Sync ERR]%s", err.Error())
				}
				err = this.syncConsumerGroupOffset()
				if err != nil {
					log.Printf("[Sync ERR]%s", err.Error())
				}
				log.Printf("[InfluxdbSyncer]end sync for %s", this.config.Zookeeper)
			}
		}
	}()

	return nil
}

func (this *InfluxdbSyncer) syncLatestOffset() error {
	worker := this.worker

	offsets, err := worker.GetLatestOffset()

	if err != nil {
		return err
	}

	pts := []client.Point{}

	for topic, partitionItem := range offsets {
		for partition, offset := range partitionItem {
			point := client.Point{
				Measurement: this.config.InfluxdbMeasurementLatestOffset,
				Tags:        map[string]string{},
				Fields: map[string]interface{}{
					"topic":     topic,
					"partition": partition,
					"value":     offset,
				},
				Time:      time.Now(),
				Precision: "s",
			}
			pts = append(pts, point)
		}
	}
	_, err = this.dbclient.Write(client.BatchPoints{
		Points:          pts,
		Database:        this.config.InfluxdbDb,
		RetentionPolicy: "default",
	})

	return err
}
func (this *InfluxdbSyncer) syncConsumerGroupOffset() error {
	worker := this.worker

	offsets, err := worker.GetConsumerGroupsOffset()

	if err != nil {
		return err
	}

	pts := []client.Point{}

	for group, topicItem := range offsets {
		for topic, partitionItem := range topicItem {
			for partition, offset := range partitionItem {
				point := client.Point{
					Measurement: this.config.InfluxdbMeasurementConsumerGroupOffset,
					Tags:        map[string]string{},
					Fields: map[string]interface{}{
						"group":     group,
						"topic":     topic,
						"partition": partition,
						"value":     offset,
					},
					Time:      time.Now(),
					Precision: "s",
				}
				pts = append(pts, point)
			}
		}

	}
	_, err = this.dbclient.Write(client.BatchPoints{
		Points:          pts,
		Database:        this.config.InfluxdbDb,
		RetentionPolicy: this.config.InfluxdbRetentionPolicy,
	})

	return err
}

func (this *InfluxdbSyncer) Close() error {

	if this.ticker != nil {
		this.ticker.Stop()
	}
	if this.worker != nil {
		this.worker.Close()
	}

	return nil
}
