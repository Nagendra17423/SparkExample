package com.test1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class WriteDf {
	public static void main(String[] args) {
		SparkSession spark=SparkSession.builder().appName("Spark_1").master("local[*]")
				.config("spark.sql.warehouse","file:///d:/sparksqldir")
				.getOrCreate();
		Dataset<Row> df=spark.read().option("header",true).option("inferSchema", true).format("csv").csv("src/main/resources/students.csv");
//		df.show();
		System.out.println("df parititon "+df.rdd().getNumPartitions()+" length "+df.rdd().partitions().length);
		
		spark.sqlContext().setConf("spark.sql.shuffle.partitions","1000");
//		df=df.repartition(functions.col("subject"));
//		df=df.filter(row->row.getAs("subject").toString().equalsIgnoreCase("Math"));
		
		df.show();
		
//		System.out.println(" spark "+spark.sql.shuffle.paritions);
		System.out.println("parition size "+df.rdd().getNumPartitions());
		df.write().partitionBy("subject").option("header",true).csv("file:///d:/sparksqldir/student_csv");
		spark.close();
		
		
	}

}
