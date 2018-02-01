package com.test.phoenix.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PhoenixTest {
	
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException{
		Connection conn = null;
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		conn = DriverManager.getConnection("jdbc:phoenix:localhost:/hbase");
		System.out.println("Got Connection");
		ResultSet rs = conn.createStatement().executeQuery("select * from emp");
		while(rs.next()){
			System.out.println("first column value: "+rs.getInt(1));
			System.out.println("second column value: "+rs.getString(2));
			System.out.println("third column value: "+rs.getString(3));
		}
		conn.close();
	}

}
