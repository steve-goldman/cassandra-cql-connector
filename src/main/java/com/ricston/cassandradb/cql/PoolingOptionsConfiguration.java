/**
 *
 * Copyright (c) Ricston Ltd.  All rights reserved.  http://www.ricston.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.ricston.cassandradb.cql;


/**
 * 
 * @author Alan Cassar, Ricston Ltd.
 *
 */
public class PoolingOptionsConfiguration {

	private Integer coreConnectionsPerHost;
	private Integer maxConnectionsPerHost;

	public Integer getCoreConnectionsPerHost() {
		return coreConnectionsPerHost;
	}

	public void setCoreConnectionsPerHost(Integer coreConnectionsPerHost) {
		this.coreConnectionsPerHost = coreConnectionsPerHost;
	}

	public Integer getMaxConnectionsPerHost() {
		return maxConnectionsPerHost;
	}

	public void setMaxConnectionsPerHost(Integer maxConnectionsPerHost) {
		this.maxConnectionsPerHost = maxConnectionsPerHost;
	}

}
