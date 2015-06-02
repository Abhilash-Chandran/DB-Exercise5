package com.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

/**
 * @author Abhilash
 *
 */
/**
 * @author Abhilash
 *
 */
/**
 * @author Abhilash
 *
 */
public class PersistenceAdmin {

	// The singleton instance of the persistence manager/admin.
	private static final PersistenceAdmin persistenceAdmin;

	// Unique transaction id.
	private int transId = 0;

	// Unique log sequence number.
	private int logSeq = 1000;

	/**
	 * This buffer holds the committed/uncommitted data which is filled in the
	 * write() method.
	 */
	private Hashtable<Integer, String> datBuff;

	/**
	 * This buffer will hold the un-committed transaction id's
	 */
	private Hashtable<Integer, List<Integer>> trnPgBuffer;

	/**
	 * Holds the committed transactions along with the corresponding pages.
	 */
	private Hashtable<Integer, List<Integer>> commTrnPgBuffer;

	static {
		persistenceAdmin = new PersistenceAdmin();
		persistenceAdmin.setDatBuff(new Hashtable<Integer, String>());
		persistenceAdmin.trnPgBuffer = new Hashtable<Integer, List<Integer>>();
		persistenceAdmin
				.setCommTrnPgBuffer(new Hashtable<Integer, List<Integer>>());
	}

	public void cleanAllFiles(String dirName) {
		File dir = new File("src/com/data/" + dirName);
		for (File file : dir.listFiles()) {
			file.delete();
		}
	}

	public synchronized Hashtable<Integer, List<Integer>> getCommTrnPgBuffer() {
		return commTrnPgBuffer;
	}

	public synchronized void setCommTrnPgBuffer(
			Hashtable<Integer, List<Integer>> commTrnPgBuffer) {
		this.commTrnPgBuffer = commTrnPgBuffer;
	}

	/**
	 * A synchronized, transactions id generator.
	 * 
	 * @return a unique transaction id as and when requested by {@code Client}
	 *         class.
	 */
	public synchronized int getNewTransId() {
		return transId++;
	}

	/**
	 * A synchronized, log sequence generator.
	 * 
	 * @return a unique log sequence as and when requested within the
	 *         {@code PersistenceAdmin} class.
	 */
	public synchronized int getNewLogSeq() {
		return logSeq++;
	}

	/**
	 * @return
	 */
	public static PersistenceAdmin getPersistenceAdmin() {
		return persistenceAdmin;
	}

	public Hashtable<Integer, String> getDatBuff() {
		return datBuff;
	}

	public void setDatBuff(Hashtable<Integer, String> datBuff) {
		this.datBuff = datBuff;
	}

	public synchronized int beginTransaction() {
		return getNewTransId();
	}

	/**
	 * This method has to do the following. 1. Write the data into the buffer
	 * datBuff, in the format pageid,logid,data as content and pageid alone as
	 * key. 2. Log the modification along with the pageid,logid,taid,and data
	 * into a new logfile for every modification. 3. verify if the buffer is
	 * full i.e, more than five committed entries and persist the data into
	 * respective permanent files.
	 *
	 *
	 * @param taid
	 *            - The transaction ID.
	 * @param pageid
	 *            - The page ID.
	 * @param data
	 *            - The user data.
	 */
	public synchronized void write(int taid, int pageid, String data) {

		// get a new log sequence number
		int logid = getNewLogSeq();

		// Write the data to buffer.
		getPersistenceAdmin().getDatBuff().put(pageid,
				pageid + "," + logid + "," + data);

		// Add the page id to the corresponding transaction id. This is helpful
		// to know what are pages to be persisted per transaction.
		if (getPersistenceAdmin().trnPgBuffer.get(taid) == null) {
			getPersistenceAdmin().trnPgBuffer.put(taid,
					new ArrayList<Integer>());
		}
		getPersistenceAdmin().trnPgBuffer.get(taid).add(pageid);

		// log the modification.
		log(logid, pageid, taid, data);

		// check for a full buffer and persist the data.
		persist();
	}

	/**
	 * This method should automatically verify the size of the buffer and
	 * persist the data for the committed transactions.
	 * 
	 */
	public synchronized void persist() {
		if (getDatBuff().size() > 5) {
			Hashtable<Integer, List<Integer>> temp = getCommTrnPgBuffer();
			for (Integer taid : temp.keySet()) {
				List<Integer> pages = temp.get(taid);
				for (Integer pgid : pages) {
					try {
						if (filehandler("src/com/data/user/" + pgid + ".txt",
								getDatBuff().get(pgid))) { // persist
							// the latest data to file.

							/* Clean the buffers */
							// getCommTrnPgBuffer().get(taid).remove(pgid);
							getDatBuff().remove(pgid);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				/*
				 * Code to clean up the buffers. Currently not working due to
				 * concurrency issue of List.
				 */
				// if (getCommTrnPgBuffer().get(taid).isEmpty()) {
				getCommTrnPgBuffer().remove(taid); // remove the old and
													// persisted
													// transactions.
				// }
			}
		}
	}

	/**
	 * Adds the transaction ID to the committed list.
	 * 
	 * @param taid
	 *            Transaction id to be stored.
	 */
	public synchronized void commit(int taid) {
		getCommTrnPgBuffer().put(taid,
				getPersistenceAdmin().trnPgBuffer.get(taid));
		getPersistenceAdmin().trnPgBuffer.remove(taid);
		persist();
	}

	/**
	 * Roll back a transaction. As of now not used.
	 * 
	 * @param taid
	 */
	public void rollback(int taid) {
		// getPersistenceAdmin().getCommitTrns().remove(taid);
	}

	/**
	 * This method is used to perform the log entry.
	 * 
	 * @param logid
	 *            - log sequence number
	 * @param pageid
	 *            - page id
	 * @param taid
	 *            - transaction id
	 * @param data
	 *            - data itself
	 */
	private void log(int logid, int pageid, int taid, String data) {
		filehandler("src/com/data/log/" + logid + ".txt", logid + "," + taid
				+ "," + pageid + "," + data);
	}

	/**
	 * This method will be used to both save the user data or the log entry to
	 * the file specified by fileName with the text as provided in fileContent.
	 * 
	 * @param fileName
	 *            Name of the file to be written.
	 * @param fileContent
	 *            - Content to be inserted into the file.
	 * @return
	 */
	private synchronized boolean filehandler(String fileName, String fileContent) {
		try {
			File file = new File(fileName);
			file.createNewFile();
			FileWriter fileWriter = new FileWriter(file);
			fileWriter.write(fileContent);
			fileWriter.flush();
			fileWriter.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * This method sequentially reads all the log files and corresponding page
	 * file and performs the recovery based on the log sequence information
	 * available.
	 */
	public void analyseAndRecover() {
		try {
			// First get a file handle for both the user and log directories.
			File logdir = new File("src/com/data/log");
			File userdir = new File("src/com/data/user");
			HashMap<Integer, Integer> pageLogMap = new HashMap<Integer, Integer>();

			BufferedReader buffReader;
			// First identify the current LSN for eage pageID.
			for (File userFile : userdir.listFiles()) {
				buffReader = new BufferedReader(new FileReader(userFile));
				String tmp = buffReader.readLine();
				if (tmp != null && !tmp.isEmpty()) { // Handle empty files.
					String[] data = tmp.split(",");
					pageLogMap.put(Integer.valueOf(data[0]),
							Integer.valueOf(data[1]));
				}
			}

			/*
			 * Identify the winner transaction/LSN for each pageid by
			 * sequentially scanning the log files and update page and log map..
			 */
			System.out.println("---------------------------------------");
			System.out.println("New Pages Created based on log entries, probably uncommitted data.");
			System.out.println("---------------------------------------");
			for (File logFile : logdir.listFiles()) {
				buffReader = new BufferedReader(new FileReader(logFile));
				String tmp = buffReader.readLine();
				if (tmp != null && !tmp.isEmpty()) { // Handle empty files.
					String[] data = tmp.split(",");
					if (pageLogMap.get(Integer.valueOf(data[2])) == null) {
						filehandler("src/com/data/user/" + data[2] + ".txt",
								data[2] + "," + data[0] + "," + data[3]);
						System.out.println("src/com/data/user/" + data[2] + ".txt");
					} else if (pageLogMap.get(Integer.valueOf(data[2])) < Integer
							.valueOf(data[0])) {
						pageLogMap.put(Integer.valueOf(data[2]),
								Integer.valueOf(data[0]));
					}
				}
			}
			System.out.println("---------------------------------------");

			/*
			 * Display the winner transactions and LSN for each page. And
			 * recover the data from the log file. No need to handle empty files
			 * because its already done in the previous map construction/winner
			 * logic.
			 */
			File logfile;
			File pageFile;
			System.out
					.println("------------------------------------------------------------------");
			System.out
					.println("PageID\t||Old Log Seq\t||Winner Log Seq||Transaction ID||Winner Found");
			System.out
					.println("------------------------------------------------------------------");
			for (Integer pgid : pageLogMap.keySet()) {
				Integer lsn = pageLogMap.get(pgid);

				logfile = new File("src/com/data/log/" + lsn + ".txt");
				pageFile = new File("src/com/data/user/" + pgid + ".txt");

				buffReader = new BufferedReader(new FileReader(logfile));
				String tmp = buffReader.readLine();
				String[] newData = tmp.split(",");
				buffReader = new BufferedReader(new FileReader(pageFile));
				String[] oldData = buffReader.readLine().split(",");
				System.out
						.println(pgid
								+ "\t||\t"
								+ oldData[1]
								+ "\t||\t"
								+ lsn
								+ "\t||\t"
								+ newData[1]
								+ "\t||\t"
								+ ((oldData[1].trim().equals(lsn + "")) ? "NO"
										: "YES"));

				// Update the user data File with new data and log sequence.
				String updateContent = pgid + "," + lsn + "," + newData[3];
				filehandler("src/com/data/user/" + pgid + ".txt", updateContent);
			}
			System.out
					.println("------------------------------------------------------------------");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * This method simply prints the user data
	 */
	public void showUserData() {
		File userdir = new File("src/com/data/user");
		System.out
				.println("---------------------------------------------------");
		System.out.println("PageID\t|| Log Sequence\t||\tUser Data");
		System.out
				.println("---------------------------------------------------");
		BufferedReader bufReader;
		try {
			for (File userFile : userdir.listFiles()) {
				bufReader = new BufferedReader(new FileReader(userFile));
				String data = bufReader.readLine();
				if (data != null && !data.isEmpty()) { // Handle empty files.
					String[] dataSp = data.split(",");
					System.out.println(dataSp[0] + "\t||\t" + dataSp[1]
							+ "\t||\t" + dataSp[2]);
				}
			}
			System.out
					.println("---------------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
