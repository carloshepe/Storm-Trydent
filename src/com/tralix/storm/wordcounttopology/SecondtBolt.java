package com.tralix.storm.wordcounttopology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Map;

public class SecondtBolt extends BaseRichBolt {
    private OutputCollector collector;
    int counter = 0;

    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        if (counter++ == 2)
            this.collector.fail(tuple);
        else {
            String sentence = tuple.getStringByField("anothersentence");
            System.out.println("SECONDBOLT:" + Arrays.asList(sentence));
            this.collector.ack(tuple);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
