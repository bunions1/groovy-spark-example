import spark.api.java.*;
import static repl.Repl.*;
import scala.Tuple2;
import repl.F;
import repl.PairF;
import categories.RddMethods



class Example{


	public static def sc;
	public static def classServer;
	public static void init(){
		classServer = new spark.HttpServer(new java.io.File("/tmp"))
		classServer.start()
		System.setProperty("spark.repl.class.uri", classServer.uri())
		//local[1]
		sc  = new JavaSparkContext("spark://lubuntu64-1.optimine.local:7077", "groovySpark", "/home/appadmin/spark", 
								   [
									   "target/libs/groovy-spark-example-2.2.0-SNAPSHOT.jar",
									   "libs/repl.jar",
									   "libs/groovy-all-2.2.0-SNAPSHOT.jar",
								   ]		 as String[]);

	}
		
	static def squaredDist(vecOne, vecTwo) {
		return [vecOne,vecTwo].transpose().collect{col -> (col[0] - col[1])**2 }.sum()
	}

	static def testMapScope  = new F({ ps ->  
										//columnwise average, transpose is zip in the groovy world
										def tmp = ps.transpose()*.sum().collect{ (it / ps.size()) }
										tmp.trimToSize()
									 })

	static void main(String[] args){
		println(new Date())
		//		repl()
		init()
		use(RddMethods){
			def textFile = sc.textFile("hdfs://lubuntu64-1.optimine.local:8020/tmp/kmeans_data.txt")
			def K = 2;
			double convergeDist = 0.5
			println("nano****************************************")			
			//parse text vectors to rdd
			def data = textFile.collect{ line ->
				def tmp = line.split(" ").collect{it.toDouble()}
				tmp.trimToSize()
				return tmp

				//				double[] doubleLine =  line.split(" ").collect{it.toDouble()} as double[]
				//				return doubleLine
			}.cache()
			println("0****************************************")			
			def centroids = data.takeSample(false, K, 42)
			def tempDist;
			println("1****************************************")			
			while(true){
				println("2****************************************")			
				//create pair rdd keyed by the index of the centroid that each vec is closes to
				def closest = data.collectEntries{ row ->
					def distances = centroids.collect{ c -> squaredDist(row, c) }
					[distances.indexOf(distances.min()), row]
				}
				println("3****************************************")			

				def pointsGroup = closest.groupByKey()
			/*
			def pointsStats = closest.reduceByKey(new F2({ vecOne, vecTwo ->  
				                                             def tmp = [vecOne, vecTwo].transpose()*.sum()
														 })
				)
			*/
				repl()
				//calc new centers
				def newCentroids = pointsGroup.mapValues(testMapScope).collectAsMap();
			println("4****************************************")			

				//distance between old and new centers
				tempDist = 0.0
				centroids.eachWithIndex{c, i -> 
					tempDist += squaredDist(c, newCentroids[i])
				}

				//copy from map to array
				newCentroids.each{k, v -> 
					centroids[k] = v 
				}
			println("5****************************************")			
				println("Finished iteration (delta = " + tempDist + ")");
				if(!(tempDist > convergeDist))
					break;
			}

			System.out.println("Final centers:");
			centroids.each{println(it)}
			println(new Date())
			//			repl()
		}
	}

}



