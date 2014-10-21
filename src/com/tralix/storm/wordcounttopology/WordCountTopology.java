package com.tralix.storm.wordcounttopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountTopology {

    static String SENTENCE_SPOUT_ID = "sentence-spout";
    static String SPLIT_BOLT_ID = "split-bolt";
    static String COUNT_BOLT_ID = "count-bolt";
    static String REPORT_BOLT_ID = "report-bolt";
    static String TOPOLOGY_NAME = "word-count-topology";
    public static void main(String[] args) throws Exception {
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
//        WordCountBolt countBolt = new WordCountBolt();
//        ReportBolt reportBolt = new ReportBolt();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, spout);
// SentenceSpout --> SplitSentenceBolt
        builder.setBolt(SPLIT_BOLT_ID, splitBolt)
                .shuffleGrouping(SENTENCE_SPOUT_ID);
// SplitSentenceBolt --> WordCountBolt
//        builder.setBolt(COUNT_BOLT_ID, countBolt)
//                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
// WordCountBolt --> ReportBolt
//        builder.setBolt(REPORT_BOLT_ID, reportBolt)
//                .globalGrouping(COUNT_BOLT_ID);
        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.
                createTopology());
        Utils.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
