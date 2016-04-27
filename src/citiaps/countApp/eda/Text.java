package citiaps.countApp.eda;

import java.util.List;

import org.apache.storm.tuple.Values;

public class Text {
	private String id;
	private String text;
	private long timestamp;

	public Text() {
		this.id = null;
		this.text = null;
		this.timestamp = 0;
	}

	public Text(String id, String text, long timestamp) {
		this.id = id;
		this.text = text;
		this.timestamp = 0;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
