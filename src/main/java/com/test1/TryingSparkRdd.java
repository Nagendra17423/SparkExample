package com.test1;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TryingSparkRdd {
	public static void main(String[] args) {
		
		Logger.getLogger("org/apache").setLevel(Level.WARN);
		System.setProperty("hadoop.home.dir", "D:/Spark/Hadoop");
		
		SparkConf conf=new SparkConf().setAppName("demo").setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		
		JavaRDD<String> rdd=sc.textFile("src/main/resources/biglog.txt");
		
//		JavaPairRDD<String,Long> prdd=
//				rdd
//				.filter(str->str.equals("WARN"))
//		.mapToPair(str->new Tuple2<String,Long>(str.split(",")[0],1l))
//		.reduceByKey((t1,t2)->t1+t2);
		
//		prdd.foreach(t -> System.out.println(t._1+" "+t._2));
		
//		ArrayList<String> list=new ArrayList<>();
//		list.add("WARN: 12/2/12");
//		list.add("ERR: 31/2/2");
//		list.add("CTC: 1/1/1");
//		list.add("WARN: 31/2/2");
//		list.add("CTC: 2/3/2");
//		
//		
//		JavaRDD<String> rdd=sc.parallelize(list);
//		rdd.collect().forEach(str->System.out.println("str "+str));
//		Tuple2 tup=rdd
//		.flatMap(str->Arrays.asList(str.split(":")).iterator())
//		.collect().forEach(System.out::println);
//		.mapToPair(str->new Tuple2<String,Long>(str.split(":")[0],1l))
//		.reduceByKey((val1,val2)->val1+val2)
//		.foreach(tup->System.out.println("key "+tup._1+" val "+tup._2));
//		.map(str->new Tuple2<String,Long>(str,1l))
//		.reduce((tup1,tup2)->
//		{
//			if(tup1._1.equals(tup2._1) && tup1._1.equals("WARN"))
//		    return new Tuple2<String,Long>(tup1._1,tup1._2+tup2._2);
//			else
//			return new Tuple2<String, Long>("WARN",tup1._1.equals("WARN") || tup2._1.equals("WARN")?1l:0l);
//		});
//		System.out.println("final output is "+tup._1+" "+tup._2);
		
//		rdd.mapToPair(str->new Tuple2<String,String>(str.split(":")[0],str.split(":")[1]))
//		.filter(tup->tup._1.equals("WARN"))
//		.foreach(tup->System.out.println("tup "+tup));
		
		
		int i=0;
		rdd.mapToPair((str)->new Tuple2<String,Long>(str.split(",")[0],1l))
		.reduceByKey((l1,l2)->l1+l2)
//		.filter(tup->{
//			
//		if(tup._1.equals("d1")) {
//			return true;}
//		return false;})
		.mapToPair(tup->new Tuple2<Long,String>(tup._2,tup._1))
		.sortByKey()
		.take(10)
		.forEach(s->System.out.println(s._1+" "+s._2));

		

		
		
		
//		.collect().forEach(tup->System.out.println("key "+tup._1+" value "+tup._2));
		
		sc.close();
		
		
		
	}

}
