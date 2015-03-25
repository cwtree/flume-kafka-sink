package org.apache.flume.cmcc.kafka;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.cmcc.kafka.CmccKafkaSink;
import org.apache.flume.cmcc.kafka.Constants;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.junit.Before;
import org.junit.Test;

public class TestCmccKafkaSink {

	private CmccKafkaSink sink;
	private Context context;
	private Channel channel;

	@Before
	public void before() {
		String partitionKey = "";
		this.context = new Context();
		this.context.put("metadata.broker.list", TestConstants.KAFKA_SERVER);
		this.context.put("serializer.class", "kafka.serializer.StringEncoder");
		this.context.put("partitioner.class",
				"org.apache.flume.cmcc.kafka.CmccPartition");
		this.context.put("request.required.acks", "0");
		this.context.put("max.message.size", "10000");
		this.context.put("producer.type", "async");
		this.context.put(Constants.CUSTOME_TOPIC_KEY_NAME,
				TestConstants.TOPIC_NAME);
		this.context.put(Constants.PARTITION_KEY_NAME, partitionKey);
		this.context.put(Constants.DEFAULT_ENCODING, "UTF-8");
		sink = new CmccKafkaSink();
		channel = new MemoryChannel();
		Configurables.configure(channel, context);
		sink.setChannel(channel);
		Configurables.configure(sink, this.context);
	}

	@Test
	public void testLifeCycle() {
		sink.start();
		sink.stop();
	}

	@Test
	public void testSinkBatchSizeEventsRight() throws EventDeliveryException {
		sink.start();
		Transaction txn = channel.getTransaction();
		txn.begin();
		for (int j = 0; j < 100; j++) {
			Event event = new SimpleEvent();
			String body = "Test." + j;
			event.setBody(body.getBytes());
			channel.put(event);
		}
		txn.commit();
		txn.close();
		// execute sink to process the events
		sink.process();
	} 
	
	@Test
	public void testSinkRight() throws EventDeliveryException {
		sink.start();
		Transaction txn = channel.getTransaction();
		txn.begin();
		for (int j = 1; j <= 1; j++) {
			Event event = new SimpleEvent();
			String body = "Test." + j;
			event.setBody(body.getBytes());
			channel.put(event);
		}
		txn.commit();
		txn.close();
		// execute sink to process the events
		sink.process();
	} 
	
	@Test
	public void testSinkNoEvent() throws EventDeliveryException {
		sink.start();
		// execute sink to process the events
		sink.process();
	}
	
	@Test
	public void testSinkException() throws EventDeliveryException {
		//sink.start();
		Transaction txn = channel.getTransaction();
		txn.begin();
		for (int j = 1; j <= 1; j++) {
			Event event = new SimpleEvent();
			String body = "Test." + j;
			event.setBody(body.getBytes());
			channel.put(event);
		}
		txn.commit();
		txn.close();
		// execute sink to process the events
		sink.process(); 
	}

}
