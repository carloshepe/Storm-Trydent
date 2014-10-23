package com.tralix.storm.trident;

import storm.trident.spout.ITridentSpout;

import java.io.Serializable;

public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<Long>, Serializable {
    private static final long serialVersionUID = 6603869949469690111L;

    @Override
    public Long initializeTransaction(final long txid, final Long prevMetadata, final Long currMetadata) {
        System.out.println("DefaultCoordinator-initializeTransaction" + txid + " prevMetadata:" + prevMetadata + " " +
                "currMetadata:" + currMetadata);
        return null;
    }

    @Override
    public void success(final long txid) {
        System.out.println("DefaultCoordinator-success" + txid);
    }

    @Override
    public boolean isReady(final long txid) {
        System.out.println("DefaultCoordinator-isReady" + txid);
        return true;
    }

    @Override
    public void close() {
        System.out.println("DefaultCoordinator-close");
    }
}
