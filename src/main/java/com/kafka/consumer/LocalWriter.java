package com.kafka.consumer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Timer;

public class LocalWriter {

	// create a new file interval, default 60*1000ms
	private long interval;
	private File lastCreateFile;
	private File writePath;
	private FileOutputStream fos;
	private final String tag;
	private final int threadNum;

	private final Object lockObject = new Object();

	public LocalWriter(String dir, String type, long interval, String tag, int threadNum) {
		this.interval = interval;
		this.tag = tag;
		this.threadNum = threadNum;

		String targetDir = dir + "/" + type;
		writePath = new File(targetDir);
		// writePath is not exist
		if (!writePath.exists() && !writePath.isDirectory()) {
			writePath.mkdir();
		}
		lastCreateFile = null;
		fos = null;

		long now = System.currentTimeMillis();
		String fileName = this.tag + "-" + String.valueOf(this.threadNum) + "-" + Long.toString(now) + ".tmp";
		File newFile = new File(writePath, fileName);
		if (!newFile.exists()) {
			try {
				newFile.createNewFile();
				lastCreateFile = newFile;
				fos = new FileOutputStream(lastCreateFile);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}

		Timer timer = new Timer();
		timer.schedule(new createFile(), this.interval, this.interval);

	}

	public LocalWriter(String dir, String type, String tag, int threadNum) {
		this(dir, type, 60 * 1000, tag, threadNum);
	}

	class createFile extends java.util.TimerTask {
		public void run() {
			long now = System.currentTimeMillis();
			String fileName = tag + "-" + String.valueOf(threadNum) + "-" + Long.toString(now) + ".tmp";
			File newFile = new File(writePath, fileName);
			if (!newFile.exists()) {
				try {
					newFile.createNewFile();
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}
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

					// change filename "1454383668315.tmp" to "1454383668315"
					String finalName = lastCreateFile.getName().split("\\.")[0];
					File finalFile = new File(writePath, finalName);
					lastCreateFile.renameTo(finalFile);

					// create new file out stream
					lastCreateFile = newFile;
					fos = null;
					try {
						fos = new FileOutputStream(lastCreateFile);
					} catch (Exception e) {
						// TODO: handle exception
					}
				}
			}
		}
	}

	public void clean() {
		synchronized (lockObject) {
			if (fos != null) {
				try {
					fos.close();

					// change filename "1454383668315.tmp" to "1454383668315"
					String finalName = lastCreateFile.getName().split("\\.")[0];
					File finalFile = new File(writePath, finalName);
					lastCreateFile.renameTo(finalFile);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void writePeriod(String content) {
		synchronized (lockObject) {
			try {
				fos.write(content.getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Exception {
		LocalWriter abc = new LocalWriter("/Users/ruochenzuo/zrc", "playlog", "logger", 1);
		abc.writePeriod("this is a test.");
		abc.writePeriod("write another message");

	}

}
