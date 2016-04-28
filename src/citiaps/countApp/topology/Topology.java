package citiaps.countApp.topology;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import citiaps.countApp.bolt.MergeBolt;
import citiaps.countApp.bolt.NumberBolt;
import citiaps.countApp.bolt.NumberWindowedBolt;
import citiaps.countApp.bolt.TextBolt;
import citiaps.countApp.spout.DefaultSpout;

public class Topology {
	private static final String TOPOLOGY_NAME = "countApp";

	public static void main(String[] args) {

		Config config = new Config();
		config.setMessageTimeoutSecs(120);
		config.setNumWorkers(1);

		TopologyBuilder builder = new TopologyBuilder();

		// Set Spout
		builder.setSpout("defaultSpout", new DefaultSpout(), 1);

		// Set Bolt
		builder.setBolt("textBolt", new TextBolt(), 1).shuffleGrouping("defaultSpout", "streamSpout");

		builder.setBolt("numberBolt", new NumberBolt(), 5).fieldsGrouping("textBolt", "streamText",
				new Fields("number"));

		// builder.setBolt("numberWindowedBolt",
		// new NumberWindowedBolt().withTumblingWindow(new Duration(5,
		// TimeUnit.SECONDS)), 5)
		// .fieldsGrouping("textBolt", "streamText", new Fields("number"));

		builder.setBolt("mergeBolt", new MergeBolt(), 1).shuffleGrouping("numberWindowedBolt", "streamNumber");

		if (args != null && args.length > 0) {
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			Utils.sleep(200000);
			cluster.shutdown();
		}

	}
}
