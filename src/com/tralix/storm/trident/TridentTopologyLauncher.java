package com.tralix.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import org.apache.log4j.xml.DOMConfigurator;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class TridentTopologyLauncher {

    public static final String TOPOLOGY_NAME = "Test-Storm";

    public static void main(String[] args) throws InterruptedException {
        DOMConfigurator.configure("log4j.xml");
        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        TridentTopology topology = new TridentTopology();
        FirstFuntionBolt firstFuntionBolt = new FirstFuntionBolt();
        SecondFuntionBolt secondFuntionBolt = new SecondFuntionBolt();
        TridentSpout tridentSpout = new TridentSpout();
        Stream inputStream = topology.newStream("event", tridentSpout)
                .each(new Fields("sentence"), firstFuntionBolt, new Fields())
                .each(new Fields("sentence"), secondFuntionBolt, new Fields());


        cluster.submitTopology(TOPOLOGY_NAME, conf, topology.build());
        Thread.sleep(4000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }
}
