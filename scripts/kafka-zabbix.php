<?php
define("HOST",gethostname());
define("DISTANCE_THRESHOLD",2000);
define("OUTFILE","/tmp/kafka_cluster_monitor");

$line = array();
$zookeepers=array(
    'cart'=>'10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181/kafka',
);

$blacklist = array(
	'cart' => array(
		'3ea39300b500528bf516957747ba9853'	=>	array('cart_op'),
		'd852d778ec3037471bbae9835047ec5f'  =>	array('cart_operation'),
	)
);

$api='http://localhost:8098/';

foreach ($zookeepers as $cluster_name => $zookeeper) {
    $url = $api . "latest_offset?zookeeper=" . $zookeeper;
    $c =file_get_contents($url);
    $c = json_decode($c,true);

    if(!is_array($c)){
    	exit(-1);
    }

    foreach($c as $topic=>$offset_arr){
        $latest=$offset_arr['total'];
        $zabbix_key = "latest_offset";
        $threshold=PHP_INT_MAX;

        $line[]=sprintf("%s kafka_monitor[%s,%s,%s,%s,%s] %s",HOST,$zabbix_key,$cluster_name,'na',$topic,$threshold,$latest);
    }
}

foreach ($zookeepers as $cluster_name => $zookeeper) {
    $url = $api . "consumer_group_distance?zookeeper=" . $zookeeper;
    $c =file_get_contents($url);
    $c = json_decode($c,true);

    if(!is_array($c)){
    	exit(-1);
    }

    foreach($c as $group=>$topic_arr){
        foreach($topic_arr as $topic=>$offset_arr){

        	if(isset($blacklist[$cluster_name][$group]) && in_array($topic, $blacklist[$cluster_name][$group])){
        		continue;
        	}

            $latest=$offset_arr['total'];
            $zabbix_key = "distance";

            $line[]=sprintf("%s kafka_monitor[%s,%s,%s,%s,%s] %s",HOST,$zabbix_key,$cluster_name,$group,$topic,DISTANCE_THRESHOLD,$latest);
        }
    }
}

file_put_contents(OUTFILE, join("\n",$line));
