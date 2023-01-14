package sparkExample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FlatMap {
	
	public static void main(String[] args) {
		
		

		SparkConf conf=new SparkConf().setAppName("PairedRdd").setMaster("local[1]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
//		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		List<String> list=new ArrayList<>();
		list.add("ERR: WEDNESDAY 4 2022");
		list.add("WARN: THURSDAY 2 2022");
		list.add("WARN: MONDAY 1 2022");
		list.add("ERR: SATURDAY 5 2022");
		JavaRDD<String> rdd=sc.parallelize(list);
		
//		JavaRDD<String> rdd1 =rdd.flatMap(d1->Arrays.asList(d1.split(" ")).iterator());
//		rdd1.foreach(d->{System.out.println(d);});
		
		rdd.flatMap(s->Arrays.asList(s.split(" ")).iterator())
//		.filter((s)-> s.endsWith("DAY"))
		.filter(s-> s.length()>1?true:false)
		.foreach(str->{System.out.println(str);});
//		.map(s->new ArrayList<String>(Arrays.asList(new String[]{s,1+""})))
//		.reduce((ArrayList<String> l1,ArrayList<String> l2)->
//		{
//			return new ArrayList<String>(Arrays.asList(new String[]{l1.get(0)+l2.get(0) , Integer.parseInt(l1.get(1))+Integer.parseInt(l2.get(1))+""}));
//		})
//		.forEach(s->System.out.println(s));
		
		
//				.collect().forEach(ArrayList<String> l->System.out.println(l));
		  
		sc.close();
		
		
		
		
		
	}

}
