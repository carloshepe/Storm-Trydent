package com.tralix.storm.trident;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class DefaultEmiter implements ITridentSpout.Emitter<Long>, Serializable {

    private static final long serialVersionUID = 6887589777878469531L;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };

    @Override
    public void emitBatch(final TransactionAttempt tx, final Long coordinatorMeta, final TridentCollector collector) {
        if (tx.getTransactionId() < sentences.length) {
            System.out.println("DefaultEmiter-emitBatch: " + tx + ":" + coordinatorMeta + ":");
            List list = new ArrayList<String>();
            list.add(new Sentence(sentences[tx.getTransactionId().intValue() - 1]));
            collector.emit(list);
            collector.emit(list);
            collector.emit(list);
            collector.emit(list);
        }
    }

    @Override
    public void success(final TransactionAttempt tx) {
        System.out.println("DefaultEmiter-success: " + tx);
    }

    @Override
    public void close() {
        System.out.println("DefaultEmiter close");
    }


}
