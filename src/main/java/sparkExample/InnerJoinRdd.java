package sparkExample;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class InnerJoinRdd {
public static void main(String[] args) {
	
	Logger.getLogger("org.apache").setLevel(Level.WARN);
	System.setProperty("hadoop.home.dir", "D:/Spark/Hadoop");
	
	SparkConf conf=new SparkConf().setAppName("InnerJoinRdd").setMaster("local[*]");
	JavaSparkContext sc=new JavaSparkContext(conf);
	Tuple2<Integer, String> tup1=new Tuple2<Integer, String>(1,"Nagendra");
	Tuple2<Integer, String> tup2=new Tuple2<Integer, String>(2,"Sarita");
	Tuple2<Integer, String> tup3=new Tuple2<Integer, String>(3,"Sunita");
	Tuple2<Integer, String> tup4=new Tuple2<Integer, String>(4,"Sonu");
	Tuple2<Integer, String> tup5=new Tuple2<Integer, String>(10,"Chotu");
	List<Tuple2<Integer,String>> list=new ArrayList<>();
	
	list.add(tup1);
	list.add(tup2);
	list.add(tup3);
	list.add(tup4);
	list.add(tup5);
	
	 Tuple2<Integer,Integer> tup21=new Tuple2<Integer, Integer>(1, 121);
	 Tuple2<Integer,Integer> tup22=new Tuple2<Integer, Integer>(2, 122);
	 Tuple2<Integer,Integer> tup23=new Tuple2<Integer, Integer>(3, 134);
	 Tuple2<Integer,Integer> tup24=new Tuple2<Integer, Integer>(4, 1221);
	 Tuple2<Integer,Integer> tup25=new Tuple2<Integer, Integer>(5, 1221);
	 Tuple2<Integer,Integer> tup26=new Tuple2<Integer, Integer>(6, 1221);
	 
	    List<Tuple2<Integer,Integer>> list1=new ArrayList<>();
		list1.add(tup21);
		list1.add(tup22);
		list1.add(tup23);
		list1.add(tup24);
		list1.add(tup25);
		list1.add(tup26);
		
	 JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(list);
	 JavaPairRDD<Integer, Integer> rdd2 = sc.parallelizePairs(list1);
	 
//	 JavaRDD rdd11=sc.parallelize(list);
//	 rdd1.collect().forEach(str->System.out.println(" "+str));
	 
//	 rdd1.collect().forEach(str->System.out.println(str));
//	 System.out.println("*******");
//	 rdd2.collect().forEach(str->System.out.println(str));
	 //inner join
//	 JavaPairRDD<Integer,Tuple2<String,Integer>> res = rdd1.join(rdd2);
	 //left outer join
//	 JavaPairRDD<Integer, Tuple2<String,Optional<Integer>>> res=rdd1.leftOuterJoin(rdd2);
	 //right outter join
//	 JavaPairRDD<Integer, Tuple2<Optional<String>,Integer>> res=rdd1.rightOuterJoin(rdd2);
	 //right outer join..
//	 res.foreach(str->System.out.println("Joining Column : "+str._1+" Name : "+str._2._1.orElse("Blank\t")+" Rank "+str._2._2));
	 
	 //cartesian join 
	 JavaPairRDD<Tuple2<Integer, String>, Tuple2<Integer, Integer>> res = rdd1.cartesian(rdd2);
	 res.foreach(str->System.out.println(str._1._1+" name "+str._1._2+" rank "+str._2._2));
	 
//	 rdd1.cogroup(rdd2).collect().forEach(str->System.out.println(str._1+" "+str._2));
	 
			 //	 res.collect().forEach(str->System.out.println("Name: "+str._2._1+" Rank : "+str._2._2.orElse(0)+" join column :"+str._1));
	
}
}
