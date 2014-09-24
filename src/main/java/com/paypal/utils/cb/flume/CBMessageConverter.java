package com.paypal.utils.cb.flume;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.spy.memcached.tapmessage.ResponseMessage;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * Converts Couchbase message into flume friendly Event format. * 
 * @author ssudhakaran
 *
 */
public class CBMessageConverter {


	public static Event convert(CBMessageFilter filter, ResponseMessage message) {
		//System.out.println("[message]"+message.getKey());
		Event event = new SimpleEvent();
		Map<String, String> headers = event.getHeaders();
		
		String key=message.getKey();
		key=key.substring(0,key.indexOf("_",key.indexOf("_")+1));
		//System.out.println("Key is "+key);
		
		if(filter.membershiptest(key)){
			String body="{ \"key\"={"+message.getKey()+"},\"value\"={"+ new String(message.getValue())+"}}";
			event.setBody(body.getBytes());
			return event;
		}else{
			return null;
		}
		
		/*
		//Logic specific to cookie . Generalize it.
		if(message.getKey().startsWith("cs_ca_")){
			headers.put("type", "analytics");
		}*/
		
		
	}



}
