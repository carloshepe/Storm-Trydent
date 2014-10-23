package com.tralix.storm.wordcounttopology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };

    private int index = 0;

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (index < sentences.length) {
            this.collector.emit(new Values(sentences[index]), index++);
        }
//        Utils.sleep(500);
    }

    @Override
    public void ack(final Object msgId) {
        System.out.println("ACK:" + msgId);
    }

    @Override
    public void fail(final Object msgId) {
        System.out.println("FAIL:" + msgId);
        this.collector.emit(new Values(sentences[(Integer) msgId]), msgId);
    }
}
