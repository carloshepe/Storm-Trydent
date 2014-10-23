package com.tralix.storm.trident;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

public class TridentSpout implements ITridentSpout<Long> {

    private static final long serialVersionUID = 584711604498475844L;
    private final DefaultCoordinator defaultCoordinator = new DefaultCoordinator();
    private final DefaultEmiter defaultEmiter = new DefaultEmiter();

    @Override
    public BatchCoordinator<Long> getCoordinator(final String txStateId, final Map conf, final TopologyContext
            context) {
        System.out.println("TridentSpout-getCoordinator");
        return defaultCoordinator;
    }

    @Override
    public Emitter<Long> getEmitter(final String txStateId, final Map conf, final TopologyContext context) {
        System.out.println("TridentSpout-getEmitter");
        return defaultEmiter;
    }

    @Override
    public Map getComponentConfiguration() {
        System.out.println("TridentSpout-getComponentConfiguration");
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("sentence");
    }
}
