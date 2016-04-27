package citiaps.countApp.bolt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NumberBolt implements IRichBolt {

	private static final long serialVersionUID = 7784329420249780555L;

	private static Logger logger = LoggerFactory.getLogger(NumberBolt.class);

	private OutputCollector outputCollector;
	private Map mapConf;

	private Timer emitTask;
	private static long TIME_DELAY = 5; /* 5seg */
	private static long EMIT_TIMEFRAME = 5; /* 5seg */

	Map<Integer, Integer> countNum;

	/**
	 * Método que se realiza cuando se inicializa el Bolt
	 */
	@Override
	public void prepare(Map mapConf, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.mapConf = mapConf;
		this.outputCollector = outputCollector;

		this.countNum = Collections.synchronizedMap(new HashMap<Integer, Integer>());

		this.emitTask = new Timer();
		this.emitTask.scheduleAtFixedRate(new EmitTask(this.countNum, this.outputCollector), TIME_DELAY * 1000,
				EMIT_TIMEFRAME * 1000);
	}

	/**
	 * Método que obtiene la siguiente tupla entrante al Bolt, el cual procesará
	 * y enviará al siguiente Bolt en caso de ser necesario
	 */
	@Override
	public void execute(Tuple tuple) {
		int number = (int) tuple.getValueByField("number");

		if (this.countNum.containsKey(number)) {
			this.countNum.put(number, (this.countNum.get(number) + 1));
		} else {
			this.countNum.put(number, 1);
		}

		this.outputCollector.ack(tuple);
	}

	/**
	 * Método que se realiza cuando se cierra el Bolt
	 */
	@Override
	public void cleanup() {
		logger.info("Close " + this.getClass().getSimpleName());

		this.emitTask.cancel();
		this.emitTask.purge();
	}

	/**
	 * Método que declara los campos que posee la tupla enviada por este bolt
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declareStream("streamNumber", new Fields("countNum"));
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

	/**
	 * Clase para poder emitir cada cierto tiempo los eventos que fueron
	 * contados por el bolt
	 */
	private class EmitTask extends TimerTask {
		private final Map<Integer, Integer> countNum;
		private final OutputCollector outputCollector;

		public EmitTask(Map<Integer, Integer> countNum, OutputCollector outputCollector) {
			this.countNum = countNum;
			this.outputCollector = outputCollector;
		}

		/**
		 * Ejecución periódica cada cierta ventana de tiempo, la cual emitirá
		 * los datos
		 */
		@Override
		public void run() {

			/*
			 * Crear un snapshot del contador, para posteriormente enviar las
			 * estadísticas
			 */
			Map<Integer, Integer> snapshotCountNum;
			synchronized (this.countNum) {
				snapshotCountNum = new HashMap<Integer, Integer>(this.countNum);
				this.countNum.clear();
			}

			this.outputCollector.emit("streamNumber", new Values(snapshotCountNum));

		}

	}

}
