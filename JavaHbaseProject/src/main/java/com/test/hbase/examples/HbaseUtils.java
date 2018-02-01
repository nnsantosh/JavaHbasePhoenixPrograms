package com.test.hbase.examples;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtils {
	
	public static void printAllValues(Result result){
		String rowId = Bytes.toString(result.getRow());
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = result.getMap();
		for(byte[] columnFamily: resultMap.keySet()){
			String columnFamilyNameStr = Bytes.toString(columnFamily);
			//System.out.println("RetrieveColumnValue.printAllValues() columnFamilyNameStr: "+columnFamilyNameStr);
			
			NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = resultMap.get(columnFamily);
			for(byte[] column: columnMap.keySet()){
				String columnNameStr = Bytes.toString(column);
				//System.out.println("RetrieveColumnValue.printAllValues() columnNameStr: "+columnNameStr);
				NavigableMap<Long, byte[]> columnValueMap = columnMap.get(column);
				for(Long timeStampVal: columnValueMap.keySet()){
					
					String columnVal = Bytes.toString(columnValueMap.get(timeStampVal));
					System.out.println("RetrieveColumnValue.printAllValues() for row id: "+ rowId + "::columnFamily: "+ columnFamilyNameStr + "::columnName: "+ columnNameStr +  "::timeStampVal: " + timeStampVal + "::columnVal: "+columnVal);
				}
				
			}
		}
	}

}
