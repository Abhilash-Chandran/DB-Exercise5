package com.data;

public class ExecuteTransactions {

	public Client client1;
	public Client client2;
	public Client client3;
	public Client client4;

	public ExecuteTransactions() {
		/*
		 * Initialize the client threads with different parameters, like
		 * specific page numbers, time delay between writes, no of repeat loops.
		 * *
		 */
		client1 = new Client("Client1", 10, 19, 100, 2);
		client2 = new Client("Client2", 20, 25, 100, 3);
		client3 = new Client("Client3", 30, 38, 100, 3);
		client4 = new Client("Client4", 40, 43, 100, 4);
	}

	public static void main(String args[]) {
		ExecuteTransactions et = new ExecuteTransactions();
		
		System.out.println("Please wait till success message.\n\n");
		// Clear the log and user data files before executing a run.
		PersistenceAdmin.getPersistenceAdmin().cleanAllFiles("user");
		PersistenceAdmin.getPersistenceAdmin().cleanAllFiles("log");

		// Start all the client threads.
		et.client1.start();
		et.client2.start();
		et.client3.start();
		et.client4.start();

		// Interrupt the clients abruptly over various timelines
		try {
			Thread.sleep(700);
		} catch (Exception e) {
			e.printStackTrace();
		}
		et.client1.interrupt();
		try {
			Thread.sleep(500);
		} catch (Exception e) {
			e.printStackTrace();
		}
		et.client2.interrupt();

		// Data buffer over-write scenario. But testing this is not really easy.
		// :)
		et.client3.interrupt();

		try {
			Thread.sleep(200);
		} catch (Exception e) {
			e.printStackTrace();
		}
		et.client4.interrupt();
		while(et.client3.isAlive()){
			
		}
		System.out.println("\n\nSuccess: You may proceed with the analysis.");
	}

}
