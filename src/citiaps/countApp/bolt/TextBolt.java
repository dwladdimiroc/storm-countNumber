package citiaps.countApp.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import citiaps.countApp.eda.Text;

public class TextBolt implements IRichBolt {

	private static final long serialVersionUID = 6101916216609388178L;

	private static Logger logger = LoggerFactory.getLogger(TextBolt.class);

	private OutputCollector outputCollector;
	private Map mapConf;

	/**
	 * Método que se realiza cuando se inicializa el Bolt
	 */
	@Override
	public void prepare(Map mapConf, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.mapConf = mapConf;
		this.outputCollector = outputCollector;
	}

	/**
	 * Método que obtiene la siguiente tupla entrante al Bolt, el cual procesará
	 * y enviará al siguiente Bolt en caso de ser necesario
	 */
	@Override
	public void execute(Tuple tuple) {
		Text texto = (Text) tuple.getValueByField("Text");
		// logger.info("La tupla es: \"" + texto.getText() + "\"");

		int number = (int) tuple.getValueByField("number");

		Values values = new Values(number);

		this.outputCollector.emit("streamText", tuple, values);
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
		outputFieldsDeclarer.declareStream("streamText", new Fields("number"));
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
