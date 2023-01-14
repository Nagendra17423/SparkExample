package sparkExample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class TextFile {
	
	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "D:/Spark/Hadoop");
//		CheckWord cw=new CheckWord();

//		SparkConf conf=new SparkConf().setAppName("PairedRdd").setMaster("local[*]");
//		JavaSparkContext sc=new JavaSparkContext(conf);
		
//		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
//		JavaRDD<String> rdd=sc.textFile("src/main/resources/Subtitles/input.txt");
//		
//		rdd.foreach(s->{
//			System.out.println(s);
//		});
		
		
//		rdd.flatMap(s->Arrays.asList(s.split(" ")).iterator())
//		.foreach(s->{System.out.println(s);});
		
		SparkConf conf=new SparkConf().setAppName("textFileApp").setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		List<String> check=new ArrayList<>();
		
		JavaRDD<String> rdd=sc.textFile("src/main/resources/Subtitles/input.txt");
		JavaPairRDD<Long,String> rdd1=rdd.map(str->str.replaceAll("[^a-zA-Z\\s]",""))
		.filter(str->str.trim().length()>0)
		.flatMap(str->Arrays.asList(str.split(" ")).iterator())
		.filter(words->{
			return !CheckWord.isBoring(words);
		})
		.mapToPair(words->{
			return new Tuple2<String,Long>(words,1l);
		})
		.reduceByKey((val1,val2)->{
			return val1+val2;
		})
		.mapToPair(tup->new Tuple2<Long,String>(tup._2,tup._1))
		.sortByKey(false);
		JavaPairRDD rdd2=rdd1.coalesce(1).sortByKey(false);
		
		rdd2.foreach(str->System.out.println(str));
		
		
		
//		.take(50)
//		.forEach(str->{
//			System.out.println(str);
//		});
		
//		rdd.flatMap(str->{
//			String res="";
//			if(!str.trim().matches("^[0-9]+"))
//			{
//				if(!str.contains(":"))
//			 {
//					String arr[]=str.split(" ");
//					for(String word:arr)
//					{
//						 String verified =cw.isNotBoring(cw.correct(word))?"":cw.correct(word);
//						check.add(verified);
//						res+=verified;
//					}
//					
//			 }
//			}
//			return Arrays.asList(res.split(" ")).iterator();
//		}).foreach(str->{
//			System.out.println(str);
//		});
		
//		rdd.foreach(str->{
////			String s[]=str.split(System.lineSeparator());
////			String words[]=s[s.length-1].split(" ");
////			System.out.println(str+" size: ");
//			if(!str.trim().matches("^[0-9]+"))
//			{
//				if(!str.contains(":"))
//			 {
//					System.out.println(str);
//					check.add(str.split(" "));
//					}
//			}
//				
//		
//		});
		
		
	}

}
