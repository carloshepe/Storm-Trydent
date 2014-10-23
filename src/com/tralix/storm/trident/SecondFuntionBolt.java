package com.tralix.storm.trident;

import backtype.storm.utils.Time;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class SecondFuntionBolt implements Function {


    private static final long serialVersionUID = -5217384570543205250L;

    int counter = 0;

    @Override
    public void execute(final TridentTuple tuple, final TridentCollector collector) {
        if (counter++ == 10) {
            collector.reportError(new RuntimeException("MyERROR"));
        }else {
            Sentence sentence = (Sentence) tuple.getValueByField("sentence");
            System.out.println("SecondFuntionBolt:" + sentence);
            System.out.println("SecondFuntionBolt:" + sentence.getSentence());

        }
        System.out.println("SecondFuntionBolt-counter:" + counter);

    }

    @Override
    public void prepare(final Map conf, final TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
