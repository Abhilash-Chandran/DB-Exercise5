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

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	/**
	 * 
	 */

	public Client(String clientId) {
		setClientId(clientId);
	}

	public void run() {
		switch (getClientId()) {
		case "Client1":
			handleTransactions(10,20,250);
			break;

		case "Client2":
			break;

		case "Client3":
			break;

		case "Client4":
			break;

		case "Client5":
			break;
		}
	}

	public void handleTransactions(int beginPgId, int endPgId, int writeDelay) {
		try {
			PersistenceAdmin persistenceAdmin = PersistenceAdmin
					.getPersistenceAdmin();
			int taid = persistenceAdmin.beginTransaction();
			for (int pageid = beginPgId; pageid <= endPgId; pageid++) {
				try {
					persistenceAdmin.write(taid, pageid,
							RandomStringUtils.random(10));
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

	}

}
