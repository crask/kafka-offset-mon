# kafka-offset-mon

[![Build Status](https://travis-ci.org/crask/kafka-offset-mon.svg?branch=master)](https://travis-ci.org/crask/kafka-offset-mon)

## 配置

```
{
    "http_server": {
        "listenAddr": ":8098",
        "patternLatestOffset": "/latest_offset",
        "patternConsumerGroupOffset": "/consumer_group_offset",
        "patternConsumerGroupDistance": "/consumer_group_distance"
    },
    "influxdbSyncers": [
        {
            "zookeeper": "127.0.0.1:2181",
            "influxdbHost": "http://127.0.0.1:8086",
            "influxdbUser": "root",
            "influxdbPassword": "root",
            "influxdbDb": "kafka_monitor",
            "influxdbRetentionPolicy": "default",
            "influxdbMeasurementLatestOffset": "latest_offset",
            "influxdbMeasurementConsumerGroupOffset": "consumer_group_offset",
            "influxdbMeasurementConsumerGroupDistance": "consumer_group_distance",
			"interval":"5s"
        }
    ]
}
```

kafka-offset-mon支持两类数据接口：

* http服务，用http_server配置，其中`listenAddr`指定了http服务监听的端口，其余`pattern*`配置，指定了对应类型的数据的获取uri。
* influxdb同步，其中`zookeeper`指定了kafka数据来源的zk地址（支持后跟chroot path的模式）。`influxdb*`配置了influxdb的相关选项。

## 使用
### 概念
目前kafka-offset-mon支持三个概念：
* latest_offset，指某个topic的各partition的最近提交的message的offset
* consumer_group_offset，指某个topic的各个consumer_group目前的消费的offset
* consumer_group_distance，指某个topic的各个consumer_group目前的消费的offset和latest的差（consumer_group_offset-latest_offset）

### http服务
如上配置，可通过`http://localhost:8098/latest_offset`来访问，返回一段json数据。

## zabbix脚本
为了方便给zabbix导出数据，使用了[/scripts/kafka-zabbix.php](/scripts/kafka-zabbix.php)

其从http服务读取数据，并组织为log文件，使用zabbix来抓取
