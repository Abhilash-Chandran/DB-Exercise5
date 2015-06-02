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

	/** A varable to control, no of transactions for each client. */
	private int noOfTransations;

	public int getNoOfTransations() {
		return noOfTransations;
	}

	public void setNoOfTransations(int noOfTransations) {
		this.noOfTransations = noOfTransations;
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

	public Client(String clientId, int beginPgId, int endPgId, int writeDelay,
			int noOfTransations) {
		setClientId(clientId);
		setBeginPgId(beginPgId);
		setEndPgId(endPgId);
		setWriteDelay(writeDelay);
		setNoOfTransations(noOfTransations);
	}

	public void run() {
		int loop = 0;
		while (loop < getNoOfTransations()) {// Continuously execute multiple
												// transactions for each
												// clients.
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
						System.out.println(getClientId()
								+ " is interupted executing transactions.");
						if (!getClientId().equals("Client3")) {
							return;
						}

						System.out
								.println("Simulating a databuffer over write for "
										+ getClientId());
						// reset the pageid, simulate to over write. :)
						pageid = getBeginPgId();
						taid = persistenceAdmin.beginTransaction();
						continue;
					}
				}
				persistenceAdmin.commit(taid);
			} catch (Exception e) {
				e.printStackTrace();
			}
			loop++;
		}
		System.out.println("Looped Transactions of " + getClientId() + " is complete");
	}
}
