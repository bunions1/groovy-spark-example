import groovy.transform.CompileStatic
import spark.api.java.function.Function;
/*
@CompileStatic
public class MyF extends Function<String, ArrayList<String>>{
	@Override
	public ArrayList<String> call(String line){
		List<Double> tmp = line.split(" ").collect{String part -> part.toDouble()}
		((ArrayList)tmp).trimToSize()
		return tmp
	}
}
*/

@CompileStatic
public class TypedClosures{


	/*
	public Function<String, ArrayList<String>> createStringSplitClosure(){
		return new MyF()
		}*/

	/*
		String line ->
			List<Double> tmp = line.split(" ").collect{String part -> part.toDouble()}
			((ArrayList)tmp).trimToSize()
			return tmp
		}
	*/

	public static Closure createClosure(){
		
		return { ArrayList<Object> vecOne, ArrayList<Object> vecTwo ->  

			ArrayList<Double> a = (ArrayList<Double>)vecOne.get(0)
			Integer aCount = (Integer)vecOne.get(1)
			ArrayList<Double> b = (ArrayList<Double>)vecTwo.get(0)
			Integer bCount = (Integer)vecTwo.get(1)

			ArrayList<Double> tmp = new ArrayList<Double>();
			for(int i = 0; i < a.size(); ++i){
				tmp.add(a.get(i) + b.get(i))
			}
			tmp.trimToSize()

			ArrayList<Object> ret = new ArrayList<Object>()
			ret.add(tmp)
			ret.add(aCount + bCount)
			return ret
		}

		/*
		return { ArrayList<Object> vecOne, ArrayList<Object> vecTwo ->  
			List<Object> tmp = [vecOne[0], vecTwo[0]].transpose().collect{ ArrayList<Double> col -> col.sum()}
			((ArrayList<Object>)tmp).trimToSize()
			return [tmp, ((double)vecOne[1]) + ((double)vecTwo[1])]
		}
		*/
	}

	public static double squaredDist(ArrayList<Double> vecOne, ArrayList<Double> vecTwo) {
		double ans = 0.0
		int i = 0;
		int length = vecOne.size()
		while(i < length){
			ans += (vecOne[i] - vecTwo[i] ) * (vecOne[i] - vecTwo[i] )
			i += 1
		}
		return ans
		
		//		return (double)([vecOne,vecTwo].transpose().collect{ArrayList<Double> col -> (col[0] - col[1])**2 }.sum())
	}


	public static Closure createCalcClosestClosure(ArrayList<ArrayList<Double>> centroids){
		return { ArrayList<Double> row ->
			List<Double> distances = centroids.collect{ ArrayList<Double> c -> squaredDist(row, c) }
			return [distances.indexOf(distances.min()), [row, 1] ]
		}
	}
	
}