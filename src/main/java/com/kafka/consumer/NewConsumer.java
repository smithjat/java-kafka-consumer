package com.kafka.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

//import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//import javax.xml.parsers.DocumentBuilder;
//import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.util.*;

 
public class NewConsumer extends Thread{
    private final ConsumerConnector consumer;
    private final String topic;
    private final Integer numThreads;
    private  ExecutorService executor;
    private final String tag;
    private final String outPath;
    private Set<String> whiteList;
    public static volatile boolean isLive = true;
    
    
    public NewConsumer(String a_zookeeper, String a_groupId, String a_topic,Integer a_threads,String a_tag,Set<String> a_whiteList,String a_outPath) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
        this.numThreads = a_threads;
        this.tag = a_tag;
        this.whiteList = a_whiteList;
        outPath = a_outPath;
        System.out.println(tag + " creat connector success!");
    }
    
    public String getTag() {
		return tag;
	}
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }
    
    
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        //System.out.println(numThreads);
        topicCountMap.put(topic, numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        System.out.println(tag + " create message streams success!");
        
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(numThreads.intValue());
        System.out.println(tag + " create excutor success!");
        // now create an object to consume the messages
        //
       
        int threadNumber = 0;
        
        for (final KafkaStream<byte[], byte[]> stream : streams) {
           try{
        	   Runnable thread = new ConsumerTask(stream, threadNumber, tag, whiteList, outPath);
        	   executor.submit(thread);
        	   System.out.println(tag + " executor submit task success!");
        	   threadNumber++;
           }catch(RuntimeException e){
        	   System.out.println("submit task exception!");
        	   shutdown();
        	   e.printStackTrace();
        	   return;
           }
        }   
        while(isLive){
        	try {
				sleep(10*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        shutdown();       
    }
 
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
 
	public static Map<String, Properties> getConfMap(String path){
		Map<String, Properties> ret = new HashMap<String, Properties>();
		try {
			/*
	    	File fXmlFile = new File(path);
	    	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	    	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	    	Document doc = dBuilder.parse(fXmlFile);
	    	*/
	    	Document doc = ParseXml.getDoc(path);
	    	doc.getDocumentElement().normalize();
	    	//System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
	    	NodeList nList = doc.getElementsByTagName("server");

	    	for (int temp = 0; temp < nList.getLength(); temp++) {

	    		Node nNode = nList.item(temp);
	    		//System.out.println("\nCurrent Element :" + nNode.getNodeName());
	    		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
	    			Element eElement = (Element) nNode;
	    			Properties prop = new Properties();
	    			prop.put("zookeeper", eElement.getElementsByTagName("zookeeper").item(0).getTextContent());
	    			prop.put("topic", eElement.getElementsByTagName("topic").item(0).getTextContent());
	    			prop.put("groupID", eElement.getElementsByTagName("groupID").item(0).getTextContent());
	    			prop.put("nThreads", eElement.getElementsByTagName("nThreads").item(0).getTextContent());
	    			
	    			ret.put(eElement.getAttribute("id"), prop);
	    		}
	    	}
	     } catch (Exception e) {
	        e.printStackTrace();
	     }
		return ret;
	}
	
	public static Set<String> getWhiteList(String path){
		Set<String> whiteList = new HashSet<String>();
		NodeList nList;
		try {
			nList = ParseXml.getNodeList(path, "type");
			for (int i = 0; i < nList.getLength(); ++i ){
				Node nNode = nList.item(i);
				whiteList.add(nNode.getTextContent());
			}
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return whiteList;
	}
	
    public static void main(String[] args) {
        
        //Map<String, Properties> conf = NewConsumer.getConfMap("config/servers.xml");
        Map<String, Properties> conf = NewConsumer.getConfMap(args[0]);
        Set<Thread> running = new HashSet<Thread>();
        Set<Thread> die = new HashSet<Thread>();
        
		Set<String> whiteList = NewConsumer.getWhiteList(args[1]);
		for(String elem : whiteList){
			System.out.println(elem);
		}
		
		String outPath = args[2];

        
        //NewConsumer example = new NewConsumer(zooKeeper, groupId, topic);
        //example.run(threads);
        
        
        
        for(String tag : conf.keySet()){
        	Properties prop = conf.get(tag);
        	Integer nThreads = Integer.valueOf(prop.getProperty("nThreads"));
            Thread thread = new NewConsumer(prop.getProperty("zookeeper"), prop.getProperty("groupID"), prop.getProperty("topic"), nThreads, tag, whiteList, outPath);
            thread.start();
            System.out.println("thread : " + tag + " start!");
            running.add(thread);
        }
             
        while(true){
        	try {
				sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	for (Thread thread : running){
        		if(thread.isAlive() == false){ 
        			die.add(thread);
        		}
        	}
        	for(Thread thread : die){
        		running.remove(thread);
        		String tag = ((NewConsumer)thread).getTag();
            	Properties prop = conf.get(tag);
            	Integer nThreads = Integer.valueOf(prop.getProperty("nThreads"));
                Thread newThread = new NewConsumer(prop.getProperty("zookeeper"), prop.getProperty("groupID"), prop.getProperty("topic"), nThreads, tag, whiteList, outPath);
        		newThread.start();
        		System.out.println("thread :" + tag + " die! Restart!");
        		running.add(newThread);
        	}
        	die.clear();  	
        }    	
        //example.shutdown();
    }
    
}