/**
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.ricston.cassandradb.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;

public class CassandraDbCqlUtils {
	
	/**
	 * Log some Cassandra Cluster information
	 * 
	 * @param cluster
	 */
	public static void logClusterInformation(Cluster cluster){
    	Metadata metadata = cluster.getMetadata();
		
    	CassandraDbCqlConnector.logger.info(String.format("Connected to cluster: %s\n", metadata.getClusterName()));
		for (Host host : metadata.getAllHosts()) {
			CassandraDbCqlConnector.logger.info(String.format("Datacenter: %s; Host: %s; Port:%d Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getSocketAddress().getPort(), host.getRack()));
		}
    }
	
	/**
	 * Convert a list of Cassandra Rows to a list of maps
	 * 
	 * @param rows
	 * @return
	 */
	public static List<Map<String, Object>> toMaps(List<Row> rows){
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		
		//if rows is empty, return empty list
		if (rows == null || rows.size() == 0){
			return result;
		}
		
		//get the column definitions from the first row
		Row firstRow = rows.get(0);
		ColumnDefinitions columDefinitions = firstRow.getColumnDefinitions();
		
		//for each row, create a map and add it to the result list
		for (Row row : rows)
		{
			Map<String, Object> mapRow = new HashMap<String, Object>();
			result.add(mapRow);
			
			//for each column defintion, get name and value, and add to map
			for (Iterator<ColumnDefinitions.Definition> i = columDefinitions.iterator(); i.hasNext();){
				ColumnDefinitions.Definition def = i.next();
				
				String name = def.getName();
				DataType type = def.getType();
				ByteBuffer bytes = row.getBytesUnsafe(name);
				Object javaObject = type.deserialize(bytes);
				
				mapRow.put(name, javaObject);
			}
		}
		
		return result;
	}

}
