
Mule Module Cassandra DB CQL
=========================

Mule Module support for Cassandra.

The Apache Cassandra database is the right choice when you need scalability and high availability without compromising performance. Linear scalability and proven fault-tolerance on commodity hardware or cloud infrastructure make it the perfect platform for mission-critical data. Cassandra's support for replicating across multiple data centers is best-in-class, providing lower latency for your users and the peace of mind of knowing that you can survive regional outages.

This module allows you to connect to Cassandra DB using the new CQL 3.0 protocol. For more information about CQL, please visit: http://cassandra.apache.org/doc/cql/CQL.html and http://www.datastax.com/docs/1.1/references/cql/index

Installation and Usage
----------------------

This project is built using Maven. Executing

```Shell
mvn install
```

in the main source directory will install the necessary dependencies and the module itself in your local repository. This also generates an update zip which you can use to install within Anypoint Studio.

Connector Configuration
===============================

Connecting to Cassandra DB can be achieved by configuring the config element like the following example. Information such as usernamd, password, secret, host, port and keypspace can all be configured at this level. Connection pooling can also be configured here as shown in the following example.

```XML
    <cassandradbcql:config name="config1" username="root" password="secret" host="127.0.0.1" port="9142" keyspace="cassandra_unit_keyspace">
    	<cassandradbcql:local-pooling-options coreConnectionsPerHost="5" maxConnectionsPerHost="15" minSimultaneousRequestsPerConnectionThreshold="50" maxSimultaneousRequestsPerConnectionThreshold="150" />
    	<cassandradbcql:remote-pooling-options coreConnectionsPerHost="2" maxConnectionsPerHost="15" minSimultaneousRequestsPerConnectionThreshold="10" maxSimultaneousRequestsPerConnectionThreshold="50" />
    </cassandradbcql:config>
```

If you need to connect to more than a single Cassandra instance, you can use the following configuration

```XML
    <cassandradbcql:config name="config2" username="root" password="secret" keyspace="cassandra_unit_keyspace">
    	<cassandradbcql:contact-points>
    		<cassandradbcql:contact-point host="127.0.0.1" port="9142"/>
    		<cassandradbcql:contact-point host="127.0.0.1" port="9143"/>
    		<cassandradbcql:contact-point host="127.0.0.1" port="9144"/>
    	</cassandradbcql:contact-points>
    </cassandradbcql:config>
```

To select elements from Cassandra DB, the following configuration can be used.

```XML
        <cassandradbcql:select cql="Select * from testTable where id = ?" config-ref="config1">
        	<cassandradbcql:params>
        		<cassandradbcql:param>java.util.UUID.fromString(payload['id'])</cassandradbcql:param>
        	</cassandradbcql:params>
        </cassandradbcql:select>
```

To use named queries, simple pass ? in your CQL statements, and parameters can be passed in later in the same order. Mule expressions are supported, however, it is very important NOT to enclose the expressions within #[].

To update a table, the following configuration can be used:

```XML
    	<cassandradbcql:update cql="UPDATE testTable SET value = ? WHERE id = ?" bulkMode="true" config-ref="config2">
        	<cassandradbcql:params>
        		<cassandradbcql:param>payload['name']</cassandradbcql:param>
        		<cassandradbcql:param>java.util.UUID.fromString(payload['id'])</cassandradbcql:param>
        	</cassandradbcql:params>
        </cassandradbcql:update>
```
As above, Mule expressions can be used without the use of #[]. Update also supports bulkMode where the payload is expected to be an instance of java.util.Collection. The CQL statement will be applied to each entry in the collection.  

For information about usage and installation you can check our documentation at http://mulesoft.github.com/mule-module-cassandradb-cql.

Reporting Issues
----------------

We use GitHub:Issues for tracking issues with this connector. You can report new issues at this link https://github.com/mulesoft/mule-module-cassandradb-cql/issues