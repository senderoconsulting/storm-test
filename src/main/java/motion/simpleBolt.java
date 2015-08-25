package motion;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class simpleBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			String message = new String((byte []) input.getValue(0));
			System.out.println("\n****************** " + message + " ***********\n");
			//collector.emit(new Values(message)); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("message"));
	}

}
