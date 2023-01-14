package sparkExample;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Demo {
	public static void main(String[] args) 
	{
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> myRdd = sc.parallelize(inputData);
		 int res=myRdd.reduce((Integer n1,Integer n2)-> n1+n2);
		 
//		 System.out.println(res);
		 
		 JavaRDD<Double> sqrtRdd=myRdd.map(n1->Math.sqrt(n1));
		 sqrtRdd.foreach((n1)->{System.out.print(n1);});
		 
//		 sqrtRdd.foreach(System.out::println);
		

		
		sc.close();
	}

}
