/**
 *
 * Copyright (c) Ricston Ltd.  All rights reserved.  http://www.ricston.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.ricston.cassandradb.cql.exception;

/**
 * 
 * @author Alan Cassar, Ricston Ltd
 *
 */
public class InvalidTypeException extends Exception{
	
	protected static final String ERROR_MSG = "%s is not support. Expected type is %s";

	/**
	 * 
	 */
	private static final long serialVersionUID = -6924196037284593129L;

	public InvalidTypeException(Class<?> type, Class<?> expectedType){
		super(String.format(ERROR_MSG, type.getName(), expectedType.getName()));
	}

}
