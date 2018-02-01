package com.test.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertColumnValue {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();

		HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("notifications"));
		if (!admin.tableExists(tableName.getTableName())) {
			System.out.println("notifications table does not exist!");
			return;
		} else {
			Table notificationsTable = connection.getTable(TableName.valueOf("notifications"));
			Put put = new Put(Bytes.toBytes("2"));
			put.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("for_user"), Bytes.toBytes("Chaz"));
			put.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("type"), Bytes.toBytes("comment"));
			notificationsTable.put(put);
			System.out.println("Succesfully inserted row id 2");
		}

	}

}
