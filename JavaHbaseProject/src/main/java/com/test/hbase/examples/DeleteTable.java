package com.test.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class DeleteTable {
	
	public static void main(String[] args) throws IOException{
		 Configuration conf = HBaseConfiguration.create();

	        Connection connection = ConnectionFactory.createConnection(conf);

	        Admin admin = connection.getAdmin();

	        HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("notifications"));
	        
	        if (admin.tableExists(tableName.getTableName())) {
	            System.out.print("Table exists, deleteing table...");
	            admin.disableTable(tableName.getTableName());
	            admin.deleteTable(tableName.getTableName());
	            System.out.println(" Done.");
	        }
	}

}
