package com.tistory.joyhong.elasticsearch.client.db;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class ConnectionDB {

	public Connection DB(String DBDriver, String url, String id, String pwd) {
		Connection con = null;
		try {
			Class.forName(DBDriver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(url, id, pwd);
			System.out.println("Database connection Ok..");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}
	
	public Connection DB(String DBDriver, String url) {
		Connection con = null;
		try {
			Class.forName(DBDriver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(url);
			System.out.println("Database connection Ok..");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}
}
