import spark.api.java.*;
import static repl.Repl.*;
import scala.Tuple2;
import repl.F;
import repl.PairF;



class Test{



	public static def sc;
	public static def classServer;
	static {
		classServer = new spark.HttpServer(new java.io.File("/tmp"))
		classServer.start()
		System.setProperty("spark.repl.class.uri", classServer.uri())
		sc  = new JavaSparkContext("spark://sparkmaster:7077", "groovySpark", "/home/appadmin/spark", 
								   [
									   "target/libs/groovy-spark-example-2.2.0-SNAPSHOT.jar",
									   "libs/repl.jar",
									   "libs/groovy-all-2.2.0-SNAPSHOT.jar",
								   ]		 as String[]);
	}

		



	static void main(String[] args){

		def textFile = sc.textFile("hdfs://namenode.local:8020/test.csv")
		result = textFile.map(new PairF({ row ->
			return new Tuple2(row, Arrays.asList([1,2,3]))

        }))
		println(result.count())
		repl()
	}

}



