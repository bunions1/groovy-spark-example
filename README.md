
# Groovy Spark

  Groovy repl support 

## Use Case Example


 write a normal spark program in groovy 

	static void main(String[] args){

		def textFile = sc.textFile("hdfs://namenode.local:8020/kmeans_data.txt")
		def result = textFile.map(new PairF({ row ->
		   //arbitary map function
			return new Tuple2(row, Arrays.asList([1,2,3]))

        }))
		println(result.count())

		//insert a call to repl() to drop into a groovy shell with context (access to all in scope variables)
		repl()
      }

	  
When execution reaches the repl() call you get a groovy prompt where can explore variables and launch new spark jobs

	 groovy:000> 
	 			 import scala.Tuple2;
				 import repl.F;
				 import repl.PairF;
				 filtered_result = reult.filter(new F({ row ->
				     //arbitrary filter function	
					 return true
				 }))

				 filtered_result.take(2
	===> [[0.1, 0.1, 0.1], [9.2, 9.2, 9.2]])



## Building 

Groovy-spark currently requires a slightly modified version of groovy. It is hoped that this can be removed someday but for now you can do the following to build groovy-spark


			 git clone https://github.com/bunions1/groovy-core.git
			 git checkout spark_shell_support
			 git submodule init
			 git submodule update

			 #this take a while as it has to build all of groovy
			 ./gradlew dist

			 #executes the example
			 ./gradlew :groovy-spark-example:run			 




			 
			 			 
