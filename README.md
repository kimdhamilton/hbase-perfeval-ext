To build:

    mvn clean package
	
Usage:

    java -cp ./hbase-perfeval-ext-0.0.1-SNAPSHOT.jar:/etc/hbase/conf.cloudera.hbase1:/usr/lib/hbase/hbase-*-security-tests.jar:`/usr/lib/hbase/bin/hbase classpath` org.apache.hadoop.hbase.PerfEvalExt

Example run:
 
     java -cp ./hbase-perfeval-ext-0.0.1-SNAPSHOT.jar:/etc/hbase/conf.cloudera.hbase1:/usr/lib/hbase/hbase-*-security-tests.jar:`/usr/lib/hbase/bin/hbase classpath` org.apache.hadoop.hbase.PerfEvalExt --presplit=10 --nomapred floatWrite 1
