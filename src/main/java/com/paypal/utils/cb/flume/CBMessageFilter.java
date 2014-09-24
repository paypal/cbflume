package com.paypal.utils.cb.flume;

import java.util.Set;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class CBMessageFilter {
	
	private BloomFilter filter = new BloomFilter(15000, 2, Hash.MURMUR_HASH);
	boolean filterEnabled=false;
	public CBMessageFilter(boolean filterEnabled, String filterPattern){
		this.filterEnabled=filterEnabled;
		
		if(this.filterEnabled){
		String[] filters=filterPattern.split(",");
		  for(String key1:filters){
			  if(key1!=null && key1.trim().length()>0){
				  Key key=new Key(key1.getBytes());
				  filter.add(key);
			  }
		  }
		}
	}
	
	public boolean membershiptest(String key){
		//return everythig if filter not enabled.
		if(!this.filterEnabled) return true;
		//System.out.println("Comparing key "+key);
		if(key==null) return false;
		return filter.membershipTest(new Key(key.getBytes()));
	}
	
	
}
