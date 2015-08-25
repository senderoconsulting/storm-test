package motion;

import java.util.UUID;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.RawMultiScheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import motion.HiveTablePartitionAction;
import motion.motionScheme;

public class motionTopology {

	public static void main(String[] args) {
		//String argument = args[0];
		
		Config config = new Config();
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		//set the number of workers
		config.setNumWorkers(2);
		
		TopologyBuilder topology = new TopologyBuilder();

		//Setup Kafka spout
		BrokerHosts hosts = new ZkHosts("10.0.0.200:2181");
		String topic = "motion";
		String zkRoot = "";
		String consumerGroupId = "group1";
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);
		spoutConfig.scheme = new RawMultiScheme();
		spoutConfig.scheme = new SchemeAsMultiScheme(new motionScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		topology.setSpout("motionKafkaSpout", kafkaSpout);	
		
		//topology.setBolt("simplebolt", new simpleBolt(),1).shuffleGrouping("motionKafkaSpout");

		// Setup HDFS bolt
		String rootPath = "/motion";
		String prefix = "motion";
		String fsUrl = "hdfs://sandbox.hortonworks.com:8020";
		String sourceMetastoreUrl = "thrift://sandbox.hortonworks.com:9083";
		String hiveStagingTableName = "motion_partition";
		String databaseName = "default";
		Float rotationTimeInMinutes = Float.valueOf("1");
		
		// this has to match the delimiter in the sql script that creates the
		// Hive table
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter("|");

		// Synchronize data buffer with the filesystem every 10 tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(10);

		// Rotate data files when they reach one MB
		//FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f,Units.MB);
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f,Units.MB);

		// Hive Partition Action
		HiveTablePartitionAction hivePartitionAction = new HiveTablePartitionAction(
				sourceMetastoreUrl, hiveStagingTableName, databaseName, fsUrl);

		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(
				rootPath + "/staging").withPrefix(prefix);

		// Instantiate the HdfsBolt
		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(fsUrl)
				.withFileNameFormat(fileNameFormat).withRecordFormat(format)
				.withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy)
				.addRotationAction(hivePartitionAction);

		// hdfsbolt.thread.count this determines how many HDFS entries are
		// created
		int hdfsBoltCount = 4;
		
		topology.setBolt("HdfsBolt", hdfsBolt,hdfsBoltCount).shuffleGrouping("motionKafkaSpout");


		try {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("motionZK", config,
					topology.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
