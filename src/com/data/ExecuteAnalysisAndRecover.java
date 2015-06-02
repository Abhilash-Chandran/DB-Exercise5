package com.data;

public class ExecuteAnalysisAndRecover {

	public ExecuteAnalysisAndRecover() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {

		// User Data Before analysis and Recovery
		PersistenceAdmin.getPersistenceAdmin().showUserData();

		// Perform analysis and recovery.
		PersistenceAdmin.getPersistenceAdmin().analyseAndRecover();

		// User Data after analysis and Recovery
		PersistenceAdmin.getPersistenceAdmin().showUserData();
	}

}
