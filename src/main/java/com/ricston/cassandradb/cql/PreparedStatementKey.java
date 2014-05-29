/**
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.ricston.cassandradb.cql;

import org.mule.util.StringUtils;

public class PreparedStatementKey {
	
	private String cql;
	private Integer batchSize;

	/**
	 * Prepared Statement Key needs cql and batch size
	 * 
	 * @param cql
	 * @param batchSize
	 */
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

	/**
	 * Override equals which returns true if cql and batch size are the same
	 */
	@Override
	public boolean equals(Object arg) {

		PreparedStatementKey preparedStatementKey = (PreparedStatementKey) arg;

		return this.batchSize.equals(preparedStatementKey.getBatchSize())
				&& StringUtils.equals(this.cql, preparedStatementKey.getCql());
	}
	
	/**
	 * Override hashCode which depends on cql and batch size
	 */
	@Override
	public int hashCode() {
		return (cql.hashCode() / 2) * (batchSize.hashCode() / 2);
	}

	public String getCql() {
		return cql;
	}

	public Integer getBatchSize() {
		return batchSize;
	}
}
