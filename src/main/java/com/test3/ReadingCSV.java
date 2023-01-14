package com.test3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.LinkedList;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ReadingCSV {
	public static void main(String[] args) {
		SparkSession spark=SparkSession.builder().appName("read_sv")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> df=spark.read().format("csv")
				.option("inferSchema",true).option("header",true)
//				.option("multiLine",true)
				.option("escape","@")
				.option("delimiter","|")
				.option("quote","'")
//				.option("columnNameOfCorruptRecord","error_generating_col")//				.options("escape)
//				.option("sep",".")
				.load("src/main/resources/read_csv.csv");
		
//		df=df.withColumn("check",functions.when(df.col("subject").equalTo("Math").and(df.col("score").gt(new Integer(50)))
//				,"Great")
//				.when(df.col("subject").equalTo("Math").and(df.col("score").gt(new Integer(20))),"Average")
//				.otherwise("let go"));
	
		
//		df=df.withColumn("check",functions.expr("case when subject = 'Math' and score > 55 then "
//				+ "'great' "
//				+ "when subject = 'Math' and score <=55 then 'not great' else "
//		+ "'average' end"));
		
		StructType schema=new StructType(new StructField[] {
				new StructField("id",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("exam_id",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("subject",DataTypes.StringType,true,Metadata.empty()),
				new StructField("year",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("quater",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("score",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("grade",DataTypes.StringType,true,Metadata.empty()),
				new StructField("_corrupt_record",DataTypes.StringType,true,Metadata.empty())
		});
		
//		Dataset<Row> df1=spark.read().format("csv")
////		.option("badRecordsPath","src/main/resources/bad_records/")
//		.option("header",true)
//		.option("inferSchema",true)
////		.schema(schema)
////		.option("quote","\'")
////		.option("escape","@")
//		.option("delimiter",",")
////		.option("mode","PERMISSIVE")
////		.option("columnNameOfCorruptRecord","error_generating_col")
//		.load("src/main/resources/read_csv1.csv");
		
		
		Dataset<Row> df1=spark.read().format("csv")
				.option("header",true)
				.option("quote","'")
				.option("escape","@")
				.option("delimiter","|")
				.option("inferschema",true)
				.load("src/main/resources/read_csv.csv");
		
//		df1.printSchema();
//		
//		df1.show();
		
		
//		df1.filter(row->row.getAs("subject").equals("Math")&& (row.getAs("grade").equals("C"))).show();
		
		LinkedList<String> list=new LinkedList<String>();
		list.add("hello");
		list.add("how r u?");
		list.add("hope u r doign great");
		list.add("if not then it wll be great");
//		System.out.println(list.getFirst()+" "+list.getLast()+" "+list.pollFirst()+" "+list.pollLast());
//		System.out.println(list.toArray().toString());
		
		list.parallelStream().map(str-> str.concat("!!"))
		.forEach(System.out::println);
		
//		df1.filter(df1.col("subject").equalTo("Math").and(df1.col("grade").equalTo("C"))).show();
		// #'student_id'|exam_center_id|subject|year|quarter|score|'grade'
		
//		df.show((int)df.count(), true);
//		df.printSchema();
//		df.explain();
//		df.describe(df.columns());
		spark.close();
		
		
	}

}
