package org.apache.flume.cmcc.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

public class CmccKafkaSink extends AbstractSink implements Configurable {

	private static final Logger log = LoggerFactory
			.getLogger(CmccKafkaSink.class);

	private Properties parameters;
	private Producer<String, String> producer; 
	// private Context context;
	private int batchSize;// 一次事务的event数量，整体提交
	private List<KeyedMessage<String, String>> messageList;
	private SinkCounter sinkCounter;

	@Override
	public Status process() {
		// TODO Auto-generated method stub
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = null;
		Event event = null;
		try {
			long processedEvent = 0;
			transaction = channel.getTransaction();
			transaction.begin();// 事务开始
			messageList.clear();
			for (; processedEvent < batchSize; processedEvent++) {
				event = channel.take();// 从channel取出一个事件
				if (event == null) {
					result = Status.BACKOFF;
					break;
				}
				sinkCounter.incrementEventDrainAttemptCount();
				// Map<String, String> headers = event.getHeaders();
				String partitionKey = parameters
						.getProperty(Constants.PARTITION_KEY_NAME);
				String topic = StringUtils.defaultIfEmpty(parameters
						.getProperty(Constants.CUSTOME_TOPIC_KEY_NAME),
						Constants.DEFAULT_TOPIC_NAME);
				String encoding = StringUtils.defaultIfEmpty(
						parameters.getProperty(Constants.ENCODING_KEY_NAME),
						Constants.DEFAULT_ENCODING);
				byte[] eventBody = event.getBody();
				String eventData = new String(eventBody, encoding);
				KeyedMessage<String, String> data = null;
				if (StringUtils.isEmpty(partitionKey)) {
					data = new KeyedMessage<String, String>(topic, eventData);
				} else {
					data = new KeyedMessage<String, String>(topic,
							partitionKey, eventData);
				}
				messageList.add(data);
				log.debug("Add data [" + eventData
						+ "] into messageList,position:" + processedEvent);
			}

			if (processedEvent == 0) {
				sinkCounter.incrementBatchEmptyCount();
				result = Status.BACKOFF;
			} else {
				if (processedEvent < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(processedEvent);
				producer.send(messageList);
				log.debug("Send MessageList to Kafka: [ message List size = "
						+ messageList.size() + ",processedEvent number = "
						+ processedEvent + "] ");
			}
			transaction.commit();// batchSize个事件处理完成，一次事务提交
			sinkCounter.addToEventDrainSuccessCount(processedEvent);
			result = Status.READY;
		} catch (Exception e) {
			String errorMsg = "Failed to publish events !";
			log.error(errorMsg, e);
			e.printStackTrace();
			result = Status.BACKOFF;
			if (transaction != null) {
				try {
					transaction.rollback();
					log.debug("transaction rollback success !");
				} catch (Exception ex) {
					log.error(errorMsg, ex);
					throw Throwables.propagate(ex);
				}
			}
			// throw new EventDeliveryException(errorMsg, e);
		} finally {
			if (transaction != null) {
				transaction.close();
			}
		}
		return result;
	}

	@Override
	public synchronized void start() {
		// TODO Auto-generated method stub
		log.info("Starting {}...", this);
		sinkCounter.start();
		super.start();
		ProducerConfig config = new ProducerConfig(this.parameters);
		this.producer = new Producer<String, String>(config);
		sinkCounter.incrementConnectionCreatedCount();
	}

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		log.debug("Cmcc Kafka sink {} stopping...", getName());
		sinkCounter.stop();
		producer.close();
		sinkCounter.incrementConnectionClosedCount();
	}

	@Override
	public void configure(Context context) {
		// TODO Auto-generated method stub
		ImmutableMap<String, String> props = context.getParameters();
		batchSize = context.getInteger(Constants.BATCH_SIZE,
				Constants.DEFAULT_BATCH_SIZE);
		messageList = new ArrayList<KeyedMessage<String, String>>(batchSize);
		parameters = new Properties();
		for (String key : props.keySet()) {
			String value = props.get(key);
			this.parameters.put(key, value);
		}
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

}
