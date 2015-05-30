package com.data;

public class ExecuteTransactions {

	public ExecuteTransactions() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String args[]) {
		Client client1 = new Client("Client1",10,19,1000,2);
		Client client2 = new Client("Client2",20,25,500,3);
		Client client3 = new Client("Client3",30,38,1000,3);
		Client client4 = new Client("Client4",40,43,800,4);
		
		client1.start();
		client2.start();
		client3.start();
		client4.start();
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		client1.interrupt();
		client2.interrupt();
		client3.interrupt();
		client4.interrupt();
	}

}
