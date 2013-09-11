import static repl.Repl.*

@Grab('org.spark-project:spark-core_2.9.3:0.7.3'){
	exclude group: 'org.apache.hadoop', module: 'hadoop-core'
}
compile ('org.apache.hadoop:hadoop-core:2.0.0-mr1-cdh4.3.0')
compile ('org.apache.hadoop:hadoop-common:2.0.0-cdh4.3.0')
compile ('org.apache.hadoop:hadoop-hdfs:2.0.0-cdh4.3.0')
compile ('org.apache.hadoop:hadoop-annotations:2.0.0-cdh4.3.0')
compile ('org.apache.hadoop:hadoop-auth:2.0.0-cdh4.3.0')
compile ('org.apache.hadoop:hadoop-client:2.0.0-mr1-cdh4.3.0')

def sc = new JavaSparkContext("spark://lubuntu64-1.example.local:7077", "groovySpark", "/home/appadmin/spark", 
							  [] as String[]);



