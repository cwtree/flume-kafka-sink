package org.apache.flume.cmcc.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmccPartition implements Partitioner {

	private static final Logger log = LoggerFactory.getLogger(CmccPartition.class);
	
	public CmccPartition(VerifiableProperties props) {
		// TODO Auto-generated constructor stub
		log.debug("");
	}

	@Override
	public int partition(Object arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

}
