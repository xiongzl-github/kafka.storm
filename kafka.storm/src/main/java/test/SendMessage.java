package test;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class SendMessage {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		props.put("zookeeper.connect", "vmhadoop001:2181,vmhadoop002:2181,vmhadoop003:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("producer.type", "async");
		props.put("compression.codec", "1");
		props.put("metadata.broker.list", "vmhadoop001:9092");
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for(int i=0;i<10;i++){
			producer.send(new KeyedMessage<String, String>("topic1",String.valueOf(i+"ddddd")));
		}
		producer.close();
		System.out.println("send over ------------------");
	}

}
