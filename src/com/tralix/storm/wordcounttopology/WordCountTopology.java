package com.tralix.storm.wordcounttopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class WordCountTopology {

    static String SENTENCE_SPOUT_ID = "sentence-spout";
    static String FIRSTBOLT_ID = "first-bolt";
    static String SECOND_ID = "second-bolt";
    static String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {
        SentenceSpout spout = new SentenceSpout();
        FirstBolt firstBolt = new FirstBolt();
        SecondtBolt secondBolt = new SecondtBolt();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, spout);
        builder.setBolt(FIRSTBOLT_ID, firstBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(SECOND_ID, secondBolt).shuffleGrouping(FIRSTBOLT_ID);
        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
