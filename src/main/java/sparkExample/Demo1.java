package sparkExample;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Demo1 {
	public static void main(String[] args) {
		
		ArrayList<Integer> list=new ArrayList<>();
		list.add(100);
		list.add(89);
		list.add(90);
		list.add(1);
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf=new SparkConf().setAppName("Demo1").setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<Integer> rdd=sc.parallelize(list);
		System.out.println("size of rdd is "+rdd.count());
		//println cannot be a part of erializable  code and code should be sent to every node containing partition of rdd
//		rdd.collect().forEach(System.out::println);
//		rdd.foreach(System.out::println);//non serizable err
		rdd.foreach(a->System.out.println(a));
		
		//tuple2 use in java with spark..
//		JavaRDD<Tuple2<Integer, Double>> tupRdd=rdd.map(n-> new Tuple2<Integer,Double>(n, Math.sqrt(n)));
//		 tupRdd.collect().forEach(n->{System.out.println(n);});
		
		//2 ways of creating a pair rdd.
		JavaPairRDD<Integer,String> p1=rdd.mapToPair(str->new Tuple2(0, str.toString()));
		JavaRDD<Tuple2<Integer,String>> p2=rdd.map(str->new Tuple2(0,str));
		
		
		 
		 
		 
		
//		JavaRDD<ArrayList<Integer>> rdd1=rdd.map((n1)-> new ArrayList<Integer>(Arrays.asList(n1,1)));
//		ArrayList<Integer> result=rdd1.reduce((list1,list2)-> new ArrayList<Integer>(Arrays.asList(list1.get(1)+list2.get(1))))));
//		System.out.println("size if "+result.get(0));
		sc.close();
		
		
	}

}
