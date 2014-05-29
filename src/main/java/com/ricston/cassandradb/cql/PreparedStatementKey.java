package com.ricston.cassandradb.cql;

import org.mule.util.StringUtils;

public class PreparedStatementKey {
	private Integer batchSize;
	private String cql;

	public PreparedStatementKey(Integer batchSize, String cql) {

		if (batchSize == null) {
			throw new IllegalArgumentException("batchSize cannot be null");
		}
		if (cql == null) {
			throw new IllegalArgumentException("cql cannot be null");
		}

		this.batchSize = batchSize;
		this.cql = cql;
	}

	@Override
	public boolean equals(Object arg) {

		PreparedStatementKey preparedStatementKey = (PreparedStatementKey) arg;

		return this.batchSize.equals(preparedStatementKey.getBatchSize())
				&& StringUtils.equals(this.cql, preparedStatementKey.getCql());
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public String getCql() {
		return cql;
	}

	public void setCql(String cql) {
		this.cql = cql;
	}

}
