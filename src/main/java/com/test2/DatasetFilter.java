package com.test2;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DatasetFilter {
	public static void main(String[] args) {
		System.setProperty("hadoop.dir","");
		SparkSession spark=SparkSession.builder().config("spark.sql.warehouse.dir","file:///d:/sparksql")
			.appName("filter_app")
				.master("local[*]")
				.getOrCreate();
	
		StructType schema=new StructType(new StructField[] {
				new StructField("Stud_id",DataTypes.DoubleType,false,Metadata.empty()),
				new StructField("Stud_center_id",DataTypes.DoubleType,false,Metadata.empty()),
				new StructField("Stud_Subject",DataTypes.StringType,false,Metadata.empty()),
				new StructField("Stud_year",DataTypes.StringType,false,Metadata.empty()),
				new StructField("Stud_quater",DataTypes.DoubleType,false,Metadata.empty()),
				new StructField("Stud_score",DataTypes.DoubleType,false,Metadata.empty()),
				new StructField("Stud_grade",DataTypes.StringType,false,Metadata.empty()),
				
		});
		Dataset<Row> df1=spark.read().format("csv").schema(schema)
				.option("header",true)
				.load("src/main/resources/Students.csv");
//		df1.filter("Stud_subject ='Math' and stud_center_id='1.0'");
//		df1.filter(row->row.getAs("Stud_Subject").toString().equalsIgnoreCase("Math") 
//				&& row.getAs("Stud_center_id").toString().equalsIgnoreCase("1.0")).show();
		
		df1.filter(
				functions.col("stud_subject").equalTo("Math").and(functions.col("stud_center_id").equalTo("1"))).show();
		
		Row r=df1.first();
		
		System.out.println(" Schema "+r.schema()+" size( no of elements )"+r.size());
//		System.out.println(functions.col("stud_subject").contains("Ma"));
//		System.out.println(functions.col("stud_score").startsWith("9"));
		df1.explain();
		df1.printSchema();
		System.out.println(r.mkString()+" "+functions.col("stud_subject").toString());
		try {
			df1.createTempView("df1");
//			df1.createOrReplaceTempView("df1");
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		spark.sql("select * from df1").show();
		
//		Dataset<Row> df2=spark.read().table("df1");
//		df2.show();
		
		Column subject=df1.apply("stud_subject");
		
//		Row r2=RowFactory.create(subject);
//		System.out.println(r2.mkString());
//		System.out.println(" input files  "+df1.inputFiles());
//		for(String s: df1.inputFiles())
//		{
//			System.out.println(s);
//		}
		
//		System.out.println(subject.asc_nulls_last());
		
//		df1.show();
		
		spark.close();
	}

}
