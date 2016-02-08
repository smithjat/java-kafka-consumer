package com.kafka.consumer;

import org.apache.hadoop.conf.Configured;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsWriter extends Configured {

	public static final String FS_PARAM_NAME = "fs.defaultFS";

	// create a new file interval, default 60*1000ms
	private long interval;
	private Path lastCreateFile;
	private Path writePath;
	private FSDataOutputStream fos;
	private final String tag;
	private final int threadNum;

	private final Object lockObject = new Object();
	private FileSystem writeFS;

	public HdfsWriter(String dir, String type, long interval, String tag, int threadNum) {
		this.interval = interval;
		this.tag = tag;
		this.threadNum = threadNum;

		/*
		 * getFileSystem will return the current user's hdfs path as the root
		 * path so relative path is relate to the current user's hdfs path
		 */

		writePath = new Path(dir + "/" + type);
		// Configuration conf = getConf();
		Configuration conf = new Configuration();
		try {
			writeFS = writePath.getFileSystem(conf);

			if (!writeFS.exists(writePath) && !writeFS.isDirectory(writePath)) {
				writeFS.mkdirs(writePath);
			}

			lastCreateFile = null;
			fos = null;

			long now = System.currentTimeMillis();
			String fileName = this.tag + "-" + String.valueOf(this.threadNum) + "-" + Long.toString(now) + ".tmp";

			Path newFile = new Path(writePath, fileName);

			if (!writeFS.exists(newFile)) {
				writeFS.createNewFile(newFile);
				lastCreateFile = newFile;
				fos = writeFS.create(lastCreateFile);

			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Timer timer = new Timer();
		timer.schedule(new createFile(), this.interval, this.interval);

	}

	public HdfsWriter(String dir, String type, String tag, int threadNum) {
		this(dir, type, 60 * 1000, tag, threadNum);
	}

	class createFile extends java.util.TimerTask {
		public void run() {
			long now = System.currentTimeMillis();
			String fileName = tag + "-" + String.valueOf(threadNum) + "-" + Long.toString(now) + ".tmp";
			Path newFile = new Path(writePath, fileName);
			try {
				if (!writeFS.exists(newFile)) {

					writeFS.createNewFile(newFile);
					synchronized (lockObject) {
						// close last file out stream first
						if (fos != null) {
							try {
								fos.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}

						// change filename "1454383668315.tmp" to
						// "1454383668315"
						String finalName = lastCreateFile.getName().split("\\.")[0];
						Path finalFile = new Path(writePath, finalName);

						writeFS.rename(lastCreateFile, finalFile);
						// create new file out stream
						lastCreateFile = newFile;
						fos = null;
						fos = writeFS.create(lastCreateFile);
					}
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}


	public void clean() {
		synchronized (lockObject) {
			try {
				if (fos != null) {
					fos.close();
				}
				// change filename "1454383668315.tmp" to "1454383668315"
				String finalName = lastCreateFile.getName().split("\\.")[0];
				Path finalFile = new Path(writePath, finalName);
				writeFS.rename(lastCreateFile, finalFile);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	public void writePeriod(String content) {
		synchronized (lockObject) {
			try {
				fos.write(content.getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("write to file failed!");
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		HdfsWriter abc = new HdfsWriter("/Users/ruochenzuo/zrc", "playlog", "logger", 1);
		abc.writePeriod("this is a test.");
		abc.writePeriod("write another message");

	}

}
