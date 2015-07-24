package main

import (
	"errors"
	"log"
	"net/url"
	"time"

	"github.com/influxdb/influxdb/client"
)

type InfluxdbSyncerConfig struct {
	zookeeper                              string
	influxdbHost                           string
	influxdbUser                           string
	influxdbPassword                       string
	influxdbDb                             string
	influxdbRetentionPolicy                string
	influxdbMeasurementLatestOffset        string
	influxdbMeasurementConsumerGroupOffset string

	interval time.Duration
}

type InfluxdbSyncer struct {
	config   *InfluxdbSyncerConfig
	worker   *Worker
	dbclient *client.Client
	ticker   *time.Ticker
}

func NewInfluxdbSyncer(config *InfluxdbSyncerConfig) *InfluxdbSyncer {
	if config.zookeeper == "" {
		config.zookeeper = "127.0.0.1:2181"
	}
	if config.influxdbHost == "" {
		config.influxdbHost = "http://127.0.0.1:8086"
	}
	if config.influxdbUser == "" {
		config.influxdbUser = "root"
	}
	if config.influxdbPassword == "" {
		config.influxdbPassword = "root"
	}
	if config.influxdbDb == "" {
		config.influxdbDb = "kafka_monitor"
	}
	if config.influxdbRetentionPolicy == "" {
		config.influxdbRetentionPolicy = "default"
	}
	if config.influxdbMeasurementConsumerGroupOffset == "" {
		config.influxdbMeasurementConsumerGroupOffset = "consumer_group_offset"
	}
	if config.influxdbMeasurementLatestOffset == "" {
		config.influxdbMeasurementLatestOffset = "latest_offset"
	}

	if config.interval == 0 {
		config.interval = time.Second * 5
	}

	s := &InfluxdbSyncer{config: config}
	return s
}

func (this *InfluxdbSyncer) Init() error {

	/* init worker */
	worker := NewWorker(this.config.zookeeper)
	err := worker.Init()
	if err != nil {
		return err
	}

	this.worker = worker

	/* init influxdb */
	influxdbUrl, err := url.Parse(this.config.influxdbHost)
	if err != nil {
		return err
	}

	conf := client.Config{
		URL:       *influxdbUrl,
		Username:  this.config.influxdbUser,
		Password:  this.config.influxdbPassword,
		UserAgent: "kafka-offset-mon",
	}
	con, err := client.NewClient(conf)
	if err != nil {
		return err
	}

	this.dbclient = con

	/* init ticker */

	this.ticker = time.NewTicker(this.config.interval)

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
				log.Printf("[InfluxdbSyncer]start sync\n")
				this.syncLatestOffset()
				this.syncConsumerGroupOffset()
				log.Printf("[InfluxdbSyncer]end sync\n")
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
				Measurement: this.config.influxdbMeasurementLatestOffset,
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
		Database:        this.config.influxdbDb,
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
					Measurement: this.config.influxdbMeasurementConsumerGroupOffset,
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
		Database:        this.config.influxdbDb,
		RetentionPolicy: this.config.influxdbRetentionPolicy,
	})

	return err
}

func (this *InfluxdbSyncer) Close() {

	if this.ticker != nil {
		this.ticker.Stop()
	}
	if this.worker != nil {
		this.worker.Close()
	}
}
