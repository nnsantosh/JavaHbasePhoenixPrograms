package com.test.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class RetrieveColumnValue {

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

			Get get = new Get(Bytes.toBytes("2"));
			get.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("for_user"));
			get.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("type"));
			Result result = notificationsTable.get(get);

			/*
			 * byte[] val1 = result.getValue(Bytes.toBytes("attributes"),
			 * Bytes.toBytes("for_user")); String strVal1 = new String(val1);
			 * System.out.println("strVal1: "+strVal1); byte[] val2 =
			 * result.getValue(Bytes.toBytes("attributes"),
			 * Bytes.toBytes("type")); String strVal2 = new String(val2);
			 * System.out.println("strVal2: "+strVal2);
			 */
			HbaseUtils.printAllValues(result);
		}
	}

}
