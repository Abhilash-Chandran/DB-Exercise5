package com.data;

public class ExecuteTransactions {

	public ExecuteTransactions() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String args[]) {
		Client client1 = new Client("Client1");
		client1.start();
		try {
			Thread.sleep(2000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		client1.interrupt();
	}

}
