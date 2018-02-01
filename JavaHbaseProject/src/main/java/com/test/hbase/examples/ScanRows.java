package com.test.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanRows {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		Connection connection = ConnectionFactory.createConnection(conf);

		Admin admin = connection.getAdmin();

		HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("notifications"));
		if (!admin.tableExists(tableName.getTableName())) {
			System.out.println("notifications table does not exist!");
			return;
		}
		else {
			Table notificationsTable = connection.getTable(TableName.valueOf("notifications"));
			if (null != notificationsTable) {
				Scan fullScan = new Scan();
				ResultScanner fullScanResultScanner = notificationsTable.getScanner(fullScan);
				for (Result res : fullScanResultScanner) {
					HbaseUtils.printAllValues(res);
				}
				fullScanResultScanner.close();

				Scan fullScanByRange = new Scan();
				fullScanByRange.setStartRow(Bytes.toBytes("1"));
				fullScanByRange.setStopRow(Bytes.toBytes("2"));
				ResultScanner fullScanResultScannerByRange = notificationsTable.getScanner(fullScanByRange);
				for (Result res : fullScanResultScannerByRange) {
					HbaseUtils.printAllValues(res);
				}
				fullScanResultScannerByRange.close();

				Scan colFamilyScan = new Scan();
				colFamilyScan.addFamily(Bytes.toBytes("metrics"));
				ResultScanner colFamilyScanResultScanner = notificationsTable.getScanner(colFamilyScan);
				for (Result res : colFamilyScanResultScanner) {
					HbaseUtils.printAllValues(res);
				}
				colFamilyScanResultScanner.close();

				Scan colScan = new Scan();
				colScan.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("for_user"));
				ResultScanner colScanResultScanner = notificationsTable.getScanner(colScan);
				for (Result res : colScanResultScanner) {
					HbaseUtils.printAllValues(res);
				}
				colScanResultScanner.close();
			}
		}
	}
}
