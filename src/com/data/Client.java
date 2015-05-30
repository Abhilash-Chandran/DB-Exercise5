/**
 * 
 */
package com.data;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * @author Abhilash
 *
 */
public class Client extends Thread {

	private String clientId;

	private int beginPgId;

	private int endPgId;

	private int writeDelay;

	private int overwrite;
	
	public int getOverwrite() {
		return overwrite;
	}

	public void setOverwrite(int overwrite) {
		this.overwrite = overwrite;
	}

	public int getBeginPgId() {
		return beginPgId;
	}

	public void setBeginPgId(int beginPgId) {
		this.beginPgId = beginPgId;
	}

	public int getEndPgId() {
		return endPgId;
	}

	public void setEndPgId(int endPgId) {
		this.endPgId = endPgId;
	}

	public int getWriteDelay() {
		return writeDelay;
	}

	public void setWriteDelay(int writeDelay) {
		this.writeDelay = writeDelay;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	/**
	 * 
	 */

	public Client(String clientId, int beginPgId, int endPgId, int writeDelay,int overwrite) {
		setClientId(clientId);
		setBeginPgId(beginPgId);
		setEndPgId(endPgId);
		setWriteDelay(writeDelay);
		setOverwrite(overwrite);
	}

	public void run() {
		int loop = 0;
		while (loop < getOverwrite()) {// Continuously execute multiple transactions for each clients.
			try {
				PersistenceAdmin persistenceAdmin = PersistenceAdmin
						.getPersistenceAdmin();
				int taid = persistenceAdmin.beginTransaction();
				for (int pageid = beginPgId; pageid <= endPgId; pageid++) {
					try {
						persistenceAdmin.write(taid, pageid,
								RandomStringUtils.randomAlphanumeric(15));
						Thread.sleep(writeDelay);
					} catch (InterruptedException e) {
						System.out
								.println("Thread interupted while sleeping between writes");
						e.printStackTrace();
					}
				}
				persistenceAdmin.commit(taid);
			} catch (Exception e) {
				e.printStackTrace();
			}
			loop++;
		}
		System.out.println("Looped Commit of "+getClientId()+"is complete");
	}
}
