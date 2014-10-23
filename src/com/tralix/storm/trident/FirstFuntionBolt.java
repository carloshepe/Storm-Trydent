package com.tralix.storm.trident;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Map;

public class FirstFuntionBolt implements Function {


    private static final long serialVersionUID = -5217384570543205250L;

    @Override
    public void execute(final TridentTuple tuple, final TridentCollector collector) {
        Sentence sentence = (Sentence)tuple.getValueByField("sentence");
        System.out.println("FirstFuntionBolt:" + sentence);
        sentence.setSentence(sentence.getSentence()+"changed");
        collector.emit(new ArrayList<Object>());
    }

    @Override
    public void prepare(final Map conf, final TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
