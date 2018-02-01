package com.test.hbase.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class BatchOperations {

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

			Put put = new Put(Bytes.toBytes("3"));
			put.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("for_user"), Bytes.toBytes("Santosh"));
			put.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("type"), Bytes.toBytes("comment"));

			Get get = new Get(Bytes.toBytes("2"));
			get.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("for_user"));
			get.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("type"));

			Get get1 = new Get(Bytes.toBytes("3"));
			get1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("for_user"));
			get1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("type"));

			Delete delete = new Delete(Bytes.toBytes("2"));
			delete.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("for_user"));

			List<Row> batch = new ArrayList<Row>();
			batch.add(put);
			batch.add(get);
			batch.add(get1);
			batch.add(delete);

			Object[] results = new Object[batch.size()];
			try {
				notificationsTable.batch(batch, results);

			} catch (Exception e) {
				System.out.println("BatchOperations.main() exception occured: " + e);
			}
			for (Object res : results) {
				Result result = (Result) res;
				if (!result.isEmpty()) {
					HbaseUtils.printAllValues(result);
				}
			}

		}

	}

}
