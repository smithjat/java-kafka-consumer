package com.kafka.consumer;

import java.io.UnsupportedEncodingException;


public class Decode{
	
    public static String[] transform(String message) {
    	try{
	    	String[] slice = message.split("&");
			String msg_type = null;
			String content= null;
			if(slice.length != 2){
				//System.out.println("invalid message!");
				//System.out.println(message);
				return null;
			}else{
				if (slice[0].contains("type")){
					try {
						msg_type = java.net.URLDecoder.decode(slice[0].subSequence("type=".length(),slice[0].length()).toString(),"utf-8").split("@")[0];
						content = java.net.URLDecoder.decode(slice[1].substring("content=".length(), slice[1].length()).toString(),"utf-8");
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						//System.out.println(message);
						return null;
					}
				}else{
					try {
						msg_type = java.net.URLDecoder.decode(slice[1].subSequence("type=".length(),slice[1].length()).toString(),"utf-8").split("@")[0];
						content = java.net.URLDecoder.decode(slice[0].substring("content=".length(), slice[0].length()).toString(),"utf-8");
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						//System.out.println(message);
						return null;
					}
				}
			}
			String[] ret = new String[2];
			ret[0] = msg_type;
			ret[1] = content;
			return ret;
    	}catch(RuntimeException e){
    		//e.printStackTrace();
    		//System.out.println(message);
    		return null;
    	}
	}
}
