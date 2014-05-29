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
	
	public static void logClusterInformation(Cluster cluster){
    	Metadata metadata = cluster.getMetadata();
		
    	CassandraDbCqlConnector.logger.info(String.format("Connected to cluster: %s\n", metadata.getClusterName()));
		for (Host host : metadata.getAllHosts()) {
			CassandraDbCqlConnector.logger.info(String.format("Datacenter: %s; Host: %s; Port:%d Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getSocketAddress().getPort(), host.getRack()));
		}
    }
	
	public static List<Map<String, Object>> toMaps(List<Row> rows){
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		
		if (rows == null || rows.size() == 0){
			return result;
		}
		
		Row firstRow = rows.get(0);
		ColumnDefinitions columDefinitions = firstRow.getColumnDefinitions();
		
		for (Row row : rows)
		{
			Map<String, Object> mapRow = new HashMap<String, Object>();
			result.add(mapRow);
			
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
