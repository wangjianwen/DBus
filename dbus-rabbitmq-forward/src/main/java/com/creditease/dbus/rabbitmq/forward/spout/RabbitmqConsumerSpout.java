package com.creditease.dbus.rabbitmq.forward.spout;


import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.rabbitmq.forward.container.DataSourceContainer;
import com.creditease.dbus.rabbitmq.forward.dao.LoadRabbitmqDao;
import com.creditease.dbus.rabbitmq.forward.dao.impl.LoadRabbitmqDaoImpl;
import com.creditease.dbus.rabbitmq.forward.domain.ExchangeProperties;
import com.creditease.dbus.rabbitmq.forward.domain.JdbcProperties;
import com.creditease.dbus.rabbitmq.forward.domain.RabbitmqProperties;
import com.creditease.dbus.rabbitmq.forward.exception.RabbitmqForwardException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.storm.shade.com.google.common.base.Strings;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * rabbitmq消费者Spout
 */
public class RabbitmqConsumerSpout extends BaseRichSpout {

    private static final Logger logger = LoggerFactory.getLogger(RabbitmqConsumerSpout.class);
    private SpoutOutputCollector collector;
    private String dsName;
    private String forwardRoot;
    private ZkService zkService;
    private Channel channel;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        String zkServers = (String) conf.get(Constants.ZOOKEEPER_SERVERS);
        this.dsName = (String) conf.get(Constants.RABBITMQ_FORWARD_TOPOLOGY_DS_NAME);
        this.forwardRoot = Constants.RABBITMQ_ROOT + "/";

        try {
            zkService = new ZkService(zkServers);
        } catch (Exception e) {
            throw new RabbitmqForwardException(String.format("连接zkServers=%s失败", zkServers));
        }

        JdbcProperties jdbcProperties = loadJdbcConfig();
        logger.info("jdbc.properties:{}", jdbcProperties);
        if(!DataSourceContainer.getInstances().initDataSource(jdbcProperties)){
            throw new RabbitmqForwardException("初始化数据源失败失败");
        }

        LoadRabbitmqDao loadRabbitmqDao = new LoadRabbitmqDaoImpl();
        RabbitmqProperties rabbitmqProperties = loadRabbitmqConfig();
        logger.info("rabbitmqProperties为{}", rabbitmqProperties);

        Rabbitmq rabbitmq = Rabbitmq.createInstance(rabbitmqProperties);
        List<ExchangeProperties> exchangePropertiesList = loadRabbitmqDao.getExchangePropertiesList(dsName);
        logger.info("本次需要转发的exchange列表为{}", exchangePropertiesList);
        channel = rabbitmq.queueDeclareAndBindMultiExchange(exchangePropertiesList);
    }

    private JdbcProperties loadJdbcConfig() {
        Properties jdbcPoolConfig;
        String jdbcPoolConfigPath = forwardRoot + dsName + "/jdbc.properties";
        try {
            jdbcPoolConfig = zkService.getProperties(jdbcPoolConfigPath);
        }catch (Exception e) {
            throw new RabbitmqForwardException(String.format("从zookeeper的%s读取连接池配置信息失败", jdbcPoolConfigPath));
        }

        Properties jdbcConnConfig;
        String jdbcConnConfigPath = Constants.COMMON_ROOT + '/' + Constants.MYSQL_PROPERTIES;
        try {
            jdbcConnConfig = zkService.getProperties(Constants.COMMON_ROOT + '/' + Constants.MYSQL_PROPERTIES);

        } catch (Exception e) {
            throw new RabbitmqForwardException(String.format("从zookeeper的%s读取jdbc连接的配置信息失败", jdbcConnConfigPath));
        }
        return buildJdbcProperties(jdbcPoolConfig, jdbcConnConfig);
    }

    private JdbcProperties buildJdbcProperties(Properties prop, Properties authProps) {
        JdbcProperties conf = new JdbcProperties();
        conf.setDriverClass(prop.getProperty("DB_DRIVER_CLASS"));
        conf.setInitialSize(Integer.parseInt(prop.getProperty("DS_INITIAL_SIZE")));
        conf.setMaxActive(Integer.parseInt(prop.getProperty("DS_MAX_ACTIVE")));
        conf.setMaxIdle(Integer.parseInt(prop.getProperty("DS_MAX_IDLE")));
        conf.setMinIdle(Integer.parseInt(prop.getProperty("DS_MIN_IDLE")));
        conf.setType(prop.getProperty("DB_TYPE"));
        conf.setKey(prop.getProperty("DB_KEY"));
        conf.setUrl(authProps.getProperty("url"));
        conf.setUserName(authProps.getProperty("username"));
        conf.setPassword(authProps.getProperty("password"));
        return conf;
    }

    private RabbitmqProperties loadRabbitmqConfig() {
        String rabbitmqConfigPath = forwardRoot + "/config.properties";
        Properties properties;
        try {
            properties = zkService.getProperties(rabbitmqConfigPath);
        } catch (Exception e) {
            throw new RuntimeException(String.format("从zookeeper的%s读取rabbitmq的配置信息失败", rabbitmqConfigPath));
        }

        RabbitmqProperties rabbitmqProperties = new RabbitmqProperties();
        rabbitmqProperties.setHost(properties.getProperty("rabbitmq.host"));
        rabbitmqProperties.setPort(Integer.parseInt(properties.getProperty("rabbitmq.port")));
        rabbitmqProperties.setUserName(properties.getProperty("rabbitmq.userName"));
        rabbitmqProperties.setPassword(properties.getProperty("rabbitmq.password"));
        return rabbitmqProperties;
    }

    @Override
    public void nextTuple() {
        try {
            channel.basicConsume(Constants.RABBITMQ_FORWARD_QUEUE, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) {
                    Map<String, String> header = new HashMap<>();
                    if (!Strings.isNullOrEmpty(envelope.getRoutingKey())) {
                        header.put("routingKey", envelope.getRoutingKey());
                    }
                    header.put("exchange", envelope.getExchange());
                    logger.info("接收到消息, exchange={}, routingKey={}", envelope.getExchange(), envelope.getRoutingKey());
                    logger.debug("接收到消息, message={}", new String(body));
                    collector.emit(new Values(header, body));
                }
            });
        } catch (IOException e) {
            throw new RabbitmqForwardException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("header", "body"));
    }
}
