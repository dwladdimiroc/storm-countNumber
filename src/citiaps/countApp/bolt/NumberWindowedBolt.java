package citiaps.countApp.bolt;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NumberWindowedBolt extends BaseWindowedBolt {

	private static final long serialVersionUID = 7784329420249780555L;

	private static Logger logger = LoggerFactory.getLogger(NumberWindowedBolt.class);

	private OutputCollector outputCollector;
	private Map mapConf;

	Map<Integer, Integer> countNum;

	/**
	 * Método que se realiza cuando se inicializa el Bolt
	 */
	@Override
	public void prepare(Map mapConf, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.mapConf = mapConf;
		this.outputCollector = outputCollector;

		this.countNum = Collections.synchronizedMap(new HashMap<Integer, Integer>());
	}

	/**
	 * Método que obtiene la siguiente tupla entrante al Bolt, el cual procesará
	 * y enviará al siguiente Bolt en caso de ser necesario
	 */
	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuplesInWindow = inputWindow.get();

		if (tuplesInWindow.size() > 0) {

			for (Tuple tuple : tuplesInWindow) {
				int number = (int) tuple.getValueByField("number");

				if (this.countNum.containsKey(number)) {
					this.countNum.put(number, (this.countNum.get(number) + 1));
				} else {
					this.countNum.put(number, 1);
				}

				this.outputCollector.ack(tuple);
			}

			Map<Integer, Integer> snapshotCountNum;
			synchronized (this.countNum) {
				snapshotCountNum = new HashMap<Integer, Integer>(this.countNum);
				this.countNum.clear();
			}

			this.outputCollector.emit("streamNumber", new Values(snapshotCountNum));
		}
	}

	/**
	 * Método que declara los campos que posee la tupla enviada por este bolt
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declareStream("streamNumber", new Fields("countNum"));
	}

}
