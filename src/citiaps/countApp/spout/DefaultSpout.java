package citiaps.countApp.spout;

import org.apache.storm.shade.com.google.common.cache.Cache;
import org.apache.storm.shade.com.google.common.cache.CacheBuilder;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import citiaps.countApp.eda.Text;

public class DefaultSpout implements IRichSpout {
	private static final long serialVersionUID = -1712766670556604983L;

	private static Logger logger = LoggerFactory.getLogger(DefaultSpout.class);

	private Map conf;
	private TopologyContext context;
	private SpoutOutputCollector collector;

	private Cache<Object, List<Object>> cache;

	// private int count;

	/** Método que se ejecuta cuando se inicializa el Spout */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.conf = conf;
		this.context = context;
		this.collector = collector;

		/*
		 * Este caché está pensando para poder almacenar las últimas tuplas en
		 * caso que exista una falla en el envío de ésta. Cosa de reenviarla en
		 * caso de ser necesario.
		 */
		this.cache = CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(5, TimeUnit.SECONDS).build();

		/* Seteo el contador */
		// this.count = 0;
	}

	/** Método que se ejecuta cuando se cierra la aplicación */
	@Override
	public void close() {
		logger.info("Close " + this.getClass());
		logger.info("Events pending: " + this.collector.getPendingCount());
		cache.cleanUp();
	}

	/** Método que se ejecuta cuando se activa la aplicación */
	@Override
	public void activate() {
		logger.info("Activate " + this.getClass().getSimpleName());
	}

	/** Método que se ejecuta cuando se desactiva la aplicación */
	@Override
	public void deactivate() {
		logger.info("Deactivate " + this.getClass().getSimpleName());
	}

	/**
	 * Método para enviar una tupla a los distintos bolts asignados en la
	 * topología
	 */
	@Override
	public void nextTuple() {
		/* Aumentamos el contador de tweets generados */
		// this.count++;

		int randNum = (int) ((Math.random() * 10) + 1);

		/* Datos de la clase Text */
		String text = new String("Este es el tweet número " + randNum);
		long timestamp = System.currentTimeMillis();

		/* Generamos el ID único de la tupla */
		String id = text.hashCode() + "-" + timestamp;

		/* Creamos la clase con sus respectivos atributos */
		Text textCurrent = new Text(id, text, timestamp);

		/*
		 * Creamos la tupla con sus respectivos valores según los campos
		 * definidos
		 */
		Values values = new Values(textCurrent, randNum);

		/* Agregamos a la caché la tupla generada */
		this.cache.put(id, values);

		/* Finalmente, enviamos el dato por el Spout */
		this.collector.emit("streamSpout", values, id);

		Utils.sleep(50);

		/* Reiniciamos el contador */
		// if ((this.count % 10) == 0) {
		// this.count = 0;
		// }
	}

	/**
	 * Método que ejecuta el ack del evento enviado por parte del spout. En caso
	 * que se emita el ack, se puede realizar algún cambio en este método, como
	 * eliminar el evento de la cola.
	 * 
	 * @param msgId
	 *            id del mensaje que realizó el ack
	 */
	@Override
	public void ack(Object msgId) {
		logger.info("Delete tuple " + msgId);
		cache.invalidate(msgId);
	}

	/**
	 * Método se ejecuta cuando existe una falla en el envío de un mensaje.
	 * 
	 * @param msgId
	 *            id del mensaje que falló su envío
	 */
	@Override
	public void fail(Object msgId) {
		logger.info("Fail tuple " + msgId);

		if (cache.getIfPresent(msgId) == null) {
			logger.info("The tuple is not in the cache...");
		} else {
			this.collector.emit("streamSpout", cache.getIfPresent(msgId), msgId);
			logger.info("Replay tuple" + cache.getIfPresent(msgId));
		}
	}

	/**
	 * Método para poder declarar los distintos campos de la tupla que será
	 * enviada por el spout
	 * 
	 * @param declarer
	 *            declaración de los campos de la tupla
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("streamSpout", new Fields("Text", "number"));
	}

	/**
	 * Get de la configuración de la topología
	 * 
	 * @return configuración
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.conf;
	}

}
