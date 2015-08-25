package motion;

import java.io.UnsupportedEncodingException;
import java.util.List;
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class motionScheme implements Scheme{

	private static final long serialVersionUID = -2990121166902741545L;

	public List<Object> deserialize(byte[] bytes) {
		try {
			String motionEvent = new String(bytes, "UTF-8");
			System.out.println("Scheme input:" + motionEvent);

			//delimiter should respect how Kafka producer is submitting events
			String[] pieces = motionEvent.split("\\|");

			String device  = pieces[0];
			String created = pieces[1];
			System.out.println("Creating a Scheme with device[" + device + "], created[" + created + "]");
			return new Values(device, created);

		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}

	}

	public Fields getOutputFields() {
		return new Fields("device", "created");

	}


}