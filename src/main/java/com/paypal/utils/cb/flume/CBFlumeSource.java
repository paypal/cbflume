package com.paypal.utils.cb.flume;

import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CBFlumeSource extends AbstractPollableSource {
	private static final Logger logger = LoggerFactory.getLogger(CBFlumeSource.class);

	private SourceCounter sourceCounter;
	private CBMessageConsumer consumer;
	Context context=null;
	

	private void log(String msg){
	//	System.out.println(msg);
	}
	@Override
	protected Status doProcess() throws EventDeliveryException {
		log("::DoProcess-------");
		boolean error = true;
		try {
			if(consumer == null) {
				consumer = createConsumer();
			}
			List<Event> events = consumer.read();
			int size = events.size();
			if(size == 0) {
				error = false;
				return Status.BACKOFF;
			}
			sourceCounter.incrementAppendBatchReceivedCount();
			sourceCounter.addToEventReceivedCount(size);
			getChannelProcessor().processEventBatch(events);
			error = false;
			sourceCounter.addToEventAcceptedCount(size);
			sourceCounter.incrementAppendBatchAcceptedCount();
			log("::DoProcess-------[RET]"+Status.READY);
			return Status.READY;
		} catch (ChannelException channelException) {
			logger.warn("Error appending event to channel. "
					+ "Channel might be full. Consider increasing the channel "
					+ "capacity or make sure the sinks perform faster.", channelException);
		} catch(Throwable throwable) {
			logger.error("Unexpected error processing events", throwable);
			if(throwable instanceof Error) {
				throw (Error) throwable;
			}
		} finally {
			if(error) {
				if(consumer != null) {
					consumer.rollback();
				}
			} else {
				if(consumer != null) {
					consumer.commit();
				}
			}
		}
		return Status.BACKOFF;
	}

	@Override
	protected void doConfigure(Context context) throws FlumeException {
		log("::DoConfigure-------");
		sourceCounter = new SourceCounter(getName());
		this.context=context;
		
		consumer=createConsumer();
	}

	@Override
	protected void doStart() throws FlumeException {
		log("::DoStart-------");

		consumer = createConsumer();
		sourceCounter.start();
	}

	@Override
	protected void doStop() throws FlumeException {
		log("::DoStop-------");
		if(consumer != null) {
			consumer.close();
			consumer = null;
		}
		sourceCounter.stop();	
	}

	/**
	 * Create couchbase consumer that can connect /extract messages.
	 * @return
	 */
	private CBMessageConsumer createConsumer()  {
	
		consumer=new CBMessageConsumer(context);
		return consumer;
	}


}
