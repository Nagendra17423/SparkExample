package com.test1;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class TryingDataFrameMethods {
	
	public static void main(String[] args) {
		
//		System.setProperty("hadoop.")
		SparkSession  spark=SparkSession.builder()
				.config("spark.sql.warehouse.dir","file:///d:/sparksqldir")
				.master("local[*]").getOrCreate();
//		SQLContext sql=spark.sqlContext();
//		sql.
		
		JavaSparkContext sc=JavaSparkContext.fromSparkContext(spark.sparkContext());
		
		List<bean> list=new ArrayList<>();
		list.add(new bean(1,"nags","abc"));
		list.add(new bean(2,"nags1","abc1"));
		list.add(new bean(3,"nags2","abc2"));
		
		
		
		JavaRDD<bean> beanRdd=sc.parallelize(list);
//		beanRdd.collect().forEach(str->System.out.println(str));
		Dataset<Row> df=spark.createDataFrame(beanRdd,bean.class);
//		df.write().saveAsTable("tab1");
		Dataset<Row> df1=spark.read()
//				.option("inferSchema", false)
				.format("parquet").parquet("file:///d:/sparksqldir/*");
		df1.printSchema();
//		df1.show();
//		df.show();
		
		
		
		
		
//		spark.
		sc.close();
		spark.close();
		
		
		
		
	}

}
