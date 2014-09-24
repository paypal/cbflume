package com.paypal.utils.cb.flume;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.TapClient;

import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapStream;

/**
 * CBMessage Consumer knows how to connect to CB and extract data. It uses TAP client to incrementally fetch new messages from Couchbase.
 * @author ssudhakaran
 *
 */
public class CBMessageConsumer {
	private static final Logger logger = LoggerFactory.getLogger(CBFlumeSource.class);
	//private static ObjectMapper objectMapper;
	public static Charset charset = Charset.forName("UTF-8");
	public static CharsetDecoder decoder = charset.newDecoder();
	private TapClient tapClient=null;
	List<URI> uri=null;
	String bucket="";
	String password="";
	String streamname="";
	String filterPattern="";
	boolean filterEnabled=false;
	CBMessageFilter filter=null;
	
	long startdate=0L;

	/**
	 * Inititalize with connection parameters
	 * @param uri
	 * @param bucket
	 * @param password
	 */
	public CBMessageConsumer(Context context){
		this.uri = new ArrayList<URI>();
		
		String servername=context.getString(Constants.CBSERVER, "").trim();
		this.bucket=context.getString(Constants.BUCKET, "").trim();
		this.streamname=context.getString(Constants.STREAMNAME, "").trim();
		this.startdate=context.getLong(Constants.STARTDATE,0L);
		this.filterPattern=context.getString(Constants.FILTERPATTERN, "").trim();
		this.filterEnabled=context.getBoolean(Constants.FILTERENABLED,false);	
		
		filter= new CBMessageFilter(this.filterEnabled,this.filterPattern);
		
		
		this.uri.add(URI.create(servername));
		initTapClient();
	}
	
	private void initTapClient(){
		System.out.println("INIT "+uri.get(0).getHost() +", bucket :"+bucket+", streamname:"+streamname+",startdate:"+startdate+",filterpattern :"+filterPattern);
		tapClient=new TapClient(uri, bucket, password);
		try {
			System.out.println("initTapClient : start date"+startdate);
			TapStream stream=tapClient.tapBackfill(null,startdate,0, TimeUnit.MINUTES);
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

	/**
	 * read messages from CB TAP 
	 * @return
	 */
	List<Event> read() {
		Event e;
		List<Event> result = new ArrayList<Event>(1);
		if(tapClient.hasMoreMessages()){
			//System.out.println("tap client has messages..");
			ResponseMessage resmessage=tapClient.getNextMessage();
			
			//System.out.println("[message]"+resmessage);
			if(resmessage!=null ) {
				e=CBMessageConverter.convert(filter,resmessage);
				if(e!=null)result.add(e);
			}
		}
		return result;
	}

	public void rollback() {
		// TODO Auto-generated method stub
		
	}

	public void commit() {
		// TODO Auto-generated method stub
		
		
	}

	public void close() {
		// TODO Auto-generated method stub
		System.out.println("Close------");
	//	tapClient.shutdown();
	}

}
