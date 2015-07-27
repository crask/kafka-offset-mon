package main

import (
	"errors"
	"fmt"

	"github.com/wvanbergen/kazoo-go"
	"gopkg.in/Shopify/sarama.v1"
)

type Worker struct {
	kazooClient *kazoo.Kazoo
	kafkaClient sarama.Client

	zookeeper string

	connected bool
}

func NewWorker(zk string) *Worker {
	return &Worker{zookeeper: zk}
}

func (this *Worker) Init() error {

	kazooConfig := kazoo.NewConfig()
	kazooClient, err := kazoo.NewKazooFromConnectionString(this.zookeeper, kazooConfig)
	if nil != err {
		return err
	}

	kafkaClientConfig := sarama.NewConfig()
	brokerList, err := kazooClient.BrokerList()
	if nil != err {
		return err
	}

	kafkaClient, err := sarama.NewClient(brokerList, kafkaClientConfig)

	if nil != err {
		return err
	}

	this.kafkaClient = kafkaClient
	this.kazooClient = kazooClient
	this.connected = true

	return nil
}

func (this *Worker) GetLatestOffset() (map[string]map[string]int64, error) {
	if this.connected == false {
		return nil, errors.New("not connected,call Init first")
	}
	rtn := map[string]map[string]int64{}
	kafkaClient := this.kafkaClient

	topics, err := kafkaClient.Topics()
	if nil != err {
		return nil, err
	}
	for _, topic := range topics {
		item := map[string]int64{}

		partitions, err := kafkaClient.Partitions(topic)
		if nil != err {
			return nil, err
		}
		var offset_total int64
		offset_total = 0
		for _, partition := range partitions {
			offset, err := kafkaClient.GetOffset(topic, partition, sarama.OffsetNewest)
			if nil != err {
				return nil, err
			}

			offset_total += offset

			item[fmt.Sprintf("%d", partition)] = offset
		}
		item["total"] = offset_total
		rtn[topic] = item

	}

	return rtn, nil
}
func (this *Worker) GetConsumerGroupsOffset() (map[string]map[string]map[string]int64, error) {

	if this.connected == false {
		return nil, errors.New("not connected,call Init first")
	}

	rtn := map[string]map[string]map[string]int64{}

	kazooClient := this.kazooClient
	kafkaClient := this.kafkaClient

	groups, err := kazooClient.Consumergroups()
	if nil != err {
		return nil, err
	}

	topics, err := kafkaClient.Topics()
	if nil != err {
		return nil, err
	}

	for _, group := range groups {
		groupItem := map[string]map[string]int64{}
		for _, topic := range topics {
			topicItem := map[string]int64{}
			partitions, err := kafkaClient.Partitions(topic)
			if nil != err {
				return nil, err
			}
			var offset_total int64
			offset_total = 0
			for _, partition := range partitions {
				offset, err := group.FetchOffset(topic, partition)
				if nil != err {
					return nil, err
				}
				offset_total += offset
				topicItem[fmt.Sprintf("%d", partition)] = offset
			}
			topicItem["total"] = offset_total
			groupItem[topic] = topicItem
		}
		rtn[group.Name] = groupItem
	}
	return rtn, nil
}

func (this *Worker) GetConsumerGroupsOffsetDistance() (map[string]map[string]map[string]int64, error) {

	if this.connected == false {
		return nil, errors.New("not connected,call Init first")
	}

	latest_offset, err := this.GetLatestOffset()

	if err != nil {
		return nil, err
	}

	rtn := map[string]map[string]map[string]int64{}

	kazooClient := this.kazooClient
	kafkaClient := this.kafkaClient

	groups, err := kazooClient.Consumergroups()
	if nil != err {
		return nil, err
	}

	topics, err := kafkaClient.Topics()
	if nil != err {
		return nil, err
	}

	for _, group := range groups {
		groupItem := map[string]map[string]int64{}
		for _, topic := range topics {
			topicItem := map[string]int64{}
			partitions, err := kafkaClient.Partitions(topic)
			if nil != err {
				return nil, err
			}
			var distance_total int64
			distance_total = 0
			for _, partition := range partitions {
				offset, err := group.FetchOffset(topic, partition)
				if nil != err {
					return nil, err
				}
				partition_str := fmt.Sprintf("%d", partition)
				distance := latest_offset[topic][partition_str] - offset
				distance_total += distance
				topicItem[fmt.Sprintf("%d", partition)] = distance
			}
			topicItem["total"] = distance_total
			groupItem[topic] = topicItem
		}
		rtn[group.Name] = groupItem
	}
	return rtn, nil
}

func (this *Worker) Close() {
	this.connected = false
	if this.connected == true {
		this.kafkaClient.Close()
		this.kazooClient.Close()
	}
}
