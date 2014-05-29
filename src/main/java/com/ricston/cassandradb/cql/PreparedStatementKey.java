package com.ricston.cassandradb.cql;

import org.mule.util.StringUtils;

public class PreparedStatementKey {
	
	private String cql;
	private Integer batchSize;

	public PreparedStatementKey(String cql, Integer batchSize) {

		if (cql == null) {
			throw new IllegalArgumentException("cql cannot be null");
		}
		
		if (batchSize == null) {
			throw new IllegalArgumentException("batchSize cannot be null");
		}
		
		this.cql = cql;
		this.batchSize = batchSize;
	}

	@Override
	public boolean equals(Object arg) {

		PreparedStatementKey preparedStatementKey = (PreparedStatementKey) arg;

		return this.batchSize.equals(preparedStatementKey.getBatchSize())
				&& StringUtils.equals(this.cql, preparedStatementKey.getCql());
	}

	public String getCql() {
		return cql;
	}

	public Integer getBatchSize() {
		return batchSize;
	}
}
