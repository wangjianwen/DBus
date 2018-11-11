package com.creditease.dbus.rabbitmq.forward;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.rabbitmq.forward.bolt.RabbitmqForwardBolt;
import com.creditease.dbus.rabbitmq.forward.spout.RabbitmqConsumerSpout;
import org.apache.commons.cli.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * rabbitmq转发消息到kafka启动类
 */
public class RabbitmqForwardTopology {
    private static final Logger logger = LoggerFactory.getLogger(RabbitmqForwardTopology.class);
    private static final String CURRENT_JAR_INFO = "dbus-rabbitmq-forward-0.5.0.jar";
    private static String zkServers;
    private static String topologyId;
    private static String forwardTopologyId;
    private static boolean runAsLocal;

    public void buildTopology (String[] args) {
        if (parseCommandArgs(args) != 0) {
            return;
        }

        logger.info("正在启动rabbitmq转发消息到kafka模块...");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RabbitmqConsumerSpout", new RabbitmqConsumerSpout(), 1);
        builder.setBolt("RabbitmqForwardBolt", new RabbitmqForwardBolt(), 1)
                .shuffleGrouping("RabbitmqConsumerSpout");

        Config conf = new Config();
        conf.put(Constants.ZOOKEEPER_SERVERS, zkServers);
        conf.put(Constants.RABBITMQ_FORWARD_TOPOLOGY_DS_NAME, topologyId);
        logger.info("zookeeper集群:{}", zkServers);
        logger.info("本次启动的数据源为{}", topologyId);
        conf.setNumWorkers(1);
        conf.setMaxSpoutPending(50);
        conf.setMessageTimeoutSecs(120);
        if (runAsLocal) {
            runAsLocal(builder, conf);
        } else {
            runAsCluster(builder, conf);
        }
    }

    private void runAsCluster(TopologyBuilder builder, Config conf){
        conf.setDebug(false);
        try {
            StormSubmitter.submitTopology(forwardTopologyId, conf, builder.createTopology());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void runAsLocal(TopologyBuilder builder, Config conf){
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(forwardTopologyId, conf, builder.createTopology());
    }


    //解析命令行传参，获取zookeeper servers和topology ID
    private int parseCommandArgs (String[] args) {
        Options options = new Options();
        options.addOption("zk", "zookeeper", true, "the zookeeper address for properties files.");
        options.addOption("tid", "topology_id", true, "the unique id as topology name and root node name in zookeeper.");
        options.addOption("l", "local", false, "run as local topology.");
        options.addOption("h", "help", false, "print usage().");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(CURRENT_JAR_INFO, options);
                return -1;
            } else {
                runAsLocal = line.hasOption("local");
                zkServers = line.getOptionValue("zookeeper");
                topologyId = line.getOptionValue("topology_id");
                forwardTopologyId = topologyId + "-rabbitmq-forward";
                if (zkServers == null || topologyId == null) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp(CURRENT_JAR_INFO, options);
                    return -1;
                }
            }
            return 0;
        } catch( ParseException exp ) {
            logger.error("Parsing failed.  Reason: " + exp.getMessage() );
            exp.printStackTrace();
            return -2;
        }
    }

    public static void main(String[] args) {
        RabbitmqForwardTopology rabbitmqForwardTopology = new RabbitmqForwardTopology();
        String[] params = new String[6];
        params[0] = "-zk";
        params[1] = "192.168.102.85:2181,192.168.102.86:2181,192.168.102.87:2181";
        params[2] = "-tid";
        params[3] = "xxx";
        params[4] = "-local";
        params[5] = "true";
        rabbitmqForwardTopology.buildTopology(params);
    }
}
