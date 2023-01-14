package com.test1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class TryingSparkSql {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("demo1").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Logger.getLogger("org.apache").setLevel(Level.WARN);

//		ArrayList<Tuple2<Integer, Integer>> l1 = new ArrayList<>();
//
//		l1.add(new Tuple2<Integer, Integer>(1, 10));
//		l1.add(new Tuple2<Integer, Integer>(2, 20));
//		l1.add(new Tuple2<Integer, Integer>(3, 30));
//		l1.add(new Tuple2<Integer, Integer>(4, 40));

//		JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(l1);
		
		JavaRDD<String> rdd2=sc.textFile("src/main/resources/biglog.txt");

//		ArrayList<Tuple2<Integer, String>> l2 = new ArrayList<>();
//
//		l2.add(new Tuple2<Integer, String>(1, "raj"));
//		l2.add(new Tuple2<Integer, String>(2, "Kalesh"));

//		JavaPairRDD<Integer, String> rdd2 = sc.parallelizePairs(l2);

//		JavaPairRDD<Integer, Tuple2<Integer, String>> res = rdd1.join(rdd2);

//		res.collect().forEach(str -> System.out.println("key " + str._1 + " value " + str._2));
		
//		JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> res=rdd1.leftOuterJoin(rdd2);
////		res.foreach(tup->System.out.println(" key "+tup._1+" value "+tup._2._2.orElse("Empty").toUpperCase()));
//		res.mapToPair(tup->new Tuple2<String,Tuple2<Integer,String>>(tup._2._2.orElse("Blank"),new Tuple2<Integer,String>(1,tup._2._2.orElse("Empty"))))
//		.reduceByKey((tup1,tup2)->new Tuple2<Integer,String>(tup1._1+tup2._1,tup1._2))
//		.foreach(tup->System.out.println("key "+tup._1+" value "+tup._2));
		
//		rdd1.fullOuterJoin(rdd2).foreach(tup->System.out.println(tup._1+" "+tup._2));
		System.out.println("no of patitions is "+rdd2.getNumPartitions());
		
		rdd2=rdd2.repartition(20);
		
		System.out.println("no of patitions is "+rdd2.getNumPartitions());
		
//		System.out.println("no of patitions is "+rdd2.getNumPartitions());
		rdd2
		.mapToPair(str->new Tuple2<String,String>(str.split(",")[0],str.split(",")[1]))
		.groupByKey()
		.foreach(tup->System.out.println(tup._1+" "+Iterables.size(tup._2)));
		
		Scanner scanner=new Scanner(System.in);
		scanner.nextInt();
		sc.close();

	}

}
