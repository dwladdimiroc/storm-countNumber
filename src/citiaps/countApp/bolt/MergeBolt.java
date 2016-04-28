package citiaps.countApp.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeBolt implements IRichBolt {

	private static final long serialVersionUID = 6383129332846743718L;

	private static Logger logger = LoggerFactory.getLogger(MergeBolt.class);

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

		this.countNum = new HashMap<Integer, Integer>();
	}

	/**
	 * Método que obtiene la siguiente tupla entrante al Bolt, el cual procesará
	 * y enviará al siguiente Bolt en caso de ser necesario
	 */
	@Override
	public void execute(Tuple tuple) {		
		Map<Integer, Integer> countNum = (Map<Integer, Integer>) tuple.getValueByField("countNum");

		for (int number : countNum.keySet()) {
			if (this.countNum.containsKey(number)) {
				int countTotal = this.countNum.get(number) + countNum.get(number);
				this.countNum.put(number, countTotal);
			} else {
				this.countNum.put(number, countNum.get(number));
			}
		}

		logger.info(this.countNum.toString());

		this.outputCollector.ack(tuple);
	}

	/**
	 * Método que se realiza cuando se cierra el Bolt
	 */
	@Override
	public void cleanup() {
		logger.info("Close " + this.getClass().getSimpleName());
	}

	/**
	 * Método que declara los campos que posee la tupla enviada por este bolt
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		/* Bolt terminal */
	}

	/**
	 * Get de la configuración de la topología
	 * 
	 * @return configuración
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return mapConf;
	}

}
