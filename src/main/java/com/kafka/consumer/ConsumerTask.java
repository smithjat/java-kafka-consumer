package com.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
//import scala.annotation.meta.companionClass;

public class ConsumerTask implements Runnable {
	private KafkaStream<byte[], byte[]> m_stream;
	private int m_threadNumber;
	private final String m_tag;
	private Set<String> m_whiteList;
	private final String m_outPath;

	// every type has a writer
	private Map<String, HdfsWriter> writer;

	public ConsumerTask(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber, String a_tag, Set<String> a_whiteList,
			String a_outPath) {
		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
		m_tag = a_tag;
		m_whiteList = a_whiteList;
		m_outPath = a_outPath;
		writer = new HashMap<String, HdfsWriter>();
		for (String type : m_whiteList) {
			HdfsWriter hw = new HdfsWriter(m_outPath, type, 10 * 60 * 1000, m_tag, m_threadNumber);
			writer.put(type, hw);
		}
		System.out.println(m_tag + " consumer exec init success!");
	}

	public void clean() {
		for (String type : writer.keySet()) {
			HdfsWriter hw = writer.get(type);
			System.out.println(m_tag + " begin to clean " + type);
			hw.clean();
		}
	}

	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		// HdfsWriter abc = new HdfsWriter("/mnt", "playlog", m_tag,
		// m_threadNumber);
		try {
			while (it.hasNext()) {
				String[] out = Decode.transform(new String(it.next().message()));
				if (out != null) {
					if (m_whiteList.contains(out[0])) {
						HdfsWriter hw = writer.get(out[0]);
						hw.writePeriod("Thread " + m_tag + " " + m_threadNumber + ": " + "[" + out[0] + "]" + out[1]);
					}
				}
				//use for interrupted
				Thread.sleep(1);
			}
			// abc.clean();
		} catch (RuntimeException e) {
			System.out.println("runtime exception!");
			e.printStackTrace();
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}

		System.out.println("Shutting down Thread: " + m_tag + " " + m_threadNumber);
		NewConsumer.isLive = false;
		clean();
	}
}