import spark.api.java.*;
import static repl.Repl.*;
import scala.Tuple2;
import repl.F;
import repl.PairF;
import repl.F2;
import categories.RddMethods

class ProfilingTools{
	public static def withTiming(String operation, Closure c){
		def start =  System.currentTimeMillis()
		def result = c()
		def end =  System.currentTimeMillis()
		println(operation  + " took: " + (end - start) + "ms")
		return result
	}
}

class Example{


	public static def sc;
	public static def classServer;
	public static void init(){
		classServer = new spark.HttpServer(new java.io.File("/tmp"))
		classServer.start()
		//		System.setProperty("spark.repl.class.uri", classServer.uri())
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



	static void main(String[] args){
		println(new Date())
		ProfilingTools.withTiming("wholedeal"){
			init()
			use(RddMethods){
				def textFile = sc.textFile("hdfs://lubuntu64-1.optimine.local:8020/tmp/kmeans_data_medium_half_72mil.txt")
				def K = 2;
				double convergeDist = 0.5
				println("nano****************************************")			
				//parse text vectors to rdd
				/*
				  def data = textFile.collect{ line ->
				  def tmp = line.split(" ").collect{it.toDouble()}
				  tmp.trimToSize()
				  return tmp

				  //				double[] doubleLine =  line.split(" ").collect{it.toDouble()} as double[]
				  //				return doubleLine
				  }.cache()
				*/
				def data = textFile.map(JavaTypedClosures.createStringSplitClosure()).cache()


				println("0****************************************")			
				def centroids = null
				ProfilingTools.withTiming("takingSample"){
					centroids = data.takeSample(false, K, 42)
					centroids = [[9.2d, 9.2d, 9.2d], [9.0d, 9.0d, 9.0d]]
				}

				def tempDist;
				println("1****************************************")			
				while(true){
					println("2****************************************")			
					//create pair rdd keyed by the index of the centroid that each vec is closes to
					def closest = null
					ProfilingTools.withTiming("making closest"){

						/*
						closest = data.collectEntries{ row ->
							def distances = centroids.collect{ c -> squaredDist(row, c) }
							[distances.indexOf(distances.min()), [row, 1] ]
						}
						*/


						closest = data.map(JavaTypedClosures.createCalcClosestClosure(centroids))
						//						closest = data.map(TypedClosures.createCalcClosestClosure(centroids))
					}
					println(new Date())				
					println("3****************************************")			

					//def pointsGroup = closest.groupByKey()

				  


					def pointsStats = closest.reduceByKey(new F2(TypedClosures.createClosure()))
					/*
					  def pointsStats = closest.reduceByKey(new F2({ vecOne, vecTwo ->  
					  def tmp = [vecOne[0], vecTwo[0]].transpose()*.sum()
					  tmp.trimToSize()
					  return [tmp, vecOne[1] + vecTwo[1]]
					  }))
					*/

					//				def durr = closest.take(1)
					println(new Date())				
					println("3.5*****************************************")
					def newPoints =  pointsStats.collectEntries{k, v -> 
						return [k, v[0].collect{it/v[1] } ]
					}.collectAsMap()

					//calc new centers
					//		def newCentroids = pointsGroup.mapValues(testMapScope).collectAsMap();
					println(new Date())				
					println("4****************************************")			

					//distance between old and new centers
					tempDist = 0.0
					centroids.eachWithIndex{c, i -> 
						tempDist += squaredDist(c, newPoints[i])
					}

					//copy from map to array
					newPoints.each{k, v -> 
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
}



