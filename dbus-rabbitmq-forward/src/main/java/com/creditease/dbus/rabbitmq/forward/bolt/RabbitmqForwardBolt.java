package com.creditease.dbus.rabbitmq.forward.bolt;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.rabbitmq.forward.dao.LoadRabbitmqDao;
import com.creditease.dbus.rabbitmq.forward.dao.impl.LoadRabbitmqDaoImpl;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class RabbitmqForwardBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(RabbitmqForwardBolt.class);

    private String outputTopic;
    private KafkaProducer<String, String> producer;

    private String zkServers;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        zkServers = (String) conf.get(Constants.ZOOKEEPER_SERVERS);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, zkServers);

        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
        LoadRabbitmqDao loadRabbitmqDao = new LoadRabbitmqDaoImpl();
        String dnsName = conf.get(Constants.RABBITMQ_FORWARD_TOPOLOGY_DS_NAME).toString();
        DataSchema schema = loadRabbitmqDao.getSchema(dnsName);
        outputTopic = schema.getTargetTopic();
        logger.info("初始化rabbitmq source={} 转发到kafka 目标topic={}", dnsName, outputTopic);
    }

    @Override
    public void execute(Tuple input) {
        Map<String, String> properties = (Map<String, String>)input.getValueByField("header");
        String exchange = properties.get("exchange");
        String routingKey = properties.get("routingKey") == null ? "":properties.get("routingKey");
        byte[] body = (byte[]) input.getValueByField("body");
        ProducerRecord<String, String> data = new ProducerRecord(outputTopic, exchange+"_" + routingKey, new String(body));
        producer.send(data);
        logger.info("转发消息到kafka成功, exchange={}, routingKey={}", exchange, routingKey);
        logger.debug("转发消息到kafka成功, message={}", new String(body));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
