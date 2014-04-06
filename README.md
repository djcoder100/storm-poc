storm-poc
==================

Taken from http://svendvanderveken.wordpress.com/2013/07/30/scalable-real-time-state-update-with-storm/

Example of basic Storm topology that updates DB persistent state. based on Java 7 and

```
        <storm.version>0.9.0.1</storm.version>
        <cassandra.version>2.0.1</cassandra.version>

```

In order to run this example, an instance of Cassandra with the following key space is required: 

```
CREATE KEYSPACE EVENT_POC WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' } ;
```