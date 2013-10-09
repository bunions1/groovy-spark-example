import spark.api.java.function.Function;
import spark.api.java.function.Function2;
import spark.api.java.function.PairFunction;
import java.util.*;
import scala.Tuple2;


public class JavaTypedClosures{
	public static Function<String, ArrayList<Double>> createStringSplitClosure(){
		return new Function<String, ArrayList<Double>>() {
			@Override
				public ArrayList<Double> call(String line){
				String[] cols = line.split(" ");
				ArrayList<Double> tmp = new ArrayList<Double>();
				for(String col: cols){
					tmp.add(Double.parseDouble(col));
				}
				tmp.trimToSize();
				return tmp;
			}
		};
	}

	public static double squaredDist(ArrayList<Double> vecOne, ArrayList<Double> vecTwo) {
		double ans = 0.0;
		int i = 0;
		int length = vecOne.size();
		while(i < length){
			ans += (vecOne.get(i) - vecTwo.get(i) ) * (vecOne.get(i) - vecTwo.get(i) );
			i += 1;
		}
		return ans;
		//		return (double)([vecOne,vecTwo].transpose().collect{ArrayList<Double> col -> (col[0] - col[1])**2 }.sum())
	}
	

	public static PairFunction<ArrayList<Double>, Integer, ArrayList<Object>> createCalcClosestClosure(final ArrayList<ArrayList<Double>> centroids){

		return new PairFunction<ArrayList<Double>, Integer, ArrayList<Object>>() {
			@Override
				public Tuple2<Integer, ArrayList<Object>> call(ArrayList<Double> row){
				ArrayList<Double> distances = new ArrayList();
				for(ArrayList<Double> c: centroids){
					distances.add(squaredDist(row, c));
				}
				ArrayList<Object> returnList = new ArrayList<Object>();
				returnList.add(row);
				returnList.add(1);
				
				return new Tuple2<Integer, ArrayList<Object>>(distances.indexOf(Collections.min(distances)), returnList);
			}
		};
	}

	/*
	public static Closure createClosure(){

		
		return { ArrayList<Object> vecOne, ArrayList<Object> vecTwo ->  
			List<Object> tmp = [vecOne[0], vecTwo[0]].transpose().collect{ ArrayList<Double> col -> col.sum()}
			((ArrayList<Object>)tmp).trimToSize()
			return [tmp, ((double)vecOne[1]) + ((double)vecTwo[1])]
		}
	
	}
	*/

}
