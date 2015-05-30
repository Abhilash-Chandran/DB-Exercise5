package com.data;

import java.io.File;
import java.io.FileWriter;
import java.util.Hashtable;

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
	private int logSeq = 0;

	/**
	 * This buffer holds the committed/uncommitted data which is filled in the
	 * write() method.
	 */
	private Hashtable<String, String> datBuff;

	/**
	 * This buffer will hold the committed transaction id's
	 */
	private Hashtable<String, String> commitBuffer;

	static {
		persistenceAdmin = new PersistenceAdmin();
		persistenceAdmin.setDatBuff(new Hashtable<String, String>());
		persistenceAdmin.commitBuffer = new Hashtable<String, String>();
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

	public Hashtable<String, String> getDatBuff() {
		return datBuff;
	}

	public void setDatBuff(Hashtable<String, String> datBuff) {
		this.datBuff = datBuff;
	}

	public int beginTransaction() {
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
	public void write(int taid, int pageid, String data) {

		// get a new log sequence number
		int logid = getNewLogSeq();

		// Write the data to buffer.
		getPersistenceAdmin().getDatBuff().put(pageid + "",
				pageid + ", " + logid + ", " + data);

		// log the modification.
		log(logid, pageid, taid, data);

		// check for a full buffer and persist the data.
		// to-do
	}

	public void persist() {
		if (getDatBuff().size() > 5) {
			for (String key : getDatBuff().keySet()) {
				
			}
		}
	}

	/**
	 * Adds the transaction ID to the committed list.
	 * 
	 * @param taid
	 *            Transaction id to be stored.
	 */
	public void commit(int taid) {
		getPersistenceAdmin().commitBuffer.put(taid + "", taid + "");
	}

	/**
	 * Roll back a transaction. As of now only removes the transaction id from
	 * the map.
	 * 
	 * @param taid
	 */
	public void rollback(int taid) {
		getPersistenceAdmin().commitBuffer.remove(taid + "");
	}

	private void log(int logid, int pageid, int taid, String data) {
		filehandler("/log/" + logid, logid + ", " + taid + ", " + ", " + pageid
				+ ", " + data);
	}

	private boolean filehandler(String fileName, String fileContent) {
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
		}
		return false;
	}
}
