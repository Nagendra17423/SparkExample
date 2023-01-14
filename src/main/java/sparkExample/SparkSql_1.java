package sparkExample;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import  static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class SparkSql_1 {
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:/Spark/Hadoop");
//		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark=SparkSession.builder().appName("SparkSql_1").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///d:/sparksqldir")
				.getOrCreate();
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/students.csv");
//		dataset.show();
//		long countObj=dataset.count();
//		System.out.println("no of object "+countObj);
//		
//		Row firstRow=dataset.first();
//		
//		String Subject = firstRow.getAs("subject").toString();
//		int score=Integer.valueOf(firstRow.getAs("score"));
//		
//		System.out.println(" Subject "+Subject+" score "+score+" "+firstRow.get(6));
		
		//filter with expression
//		Dataset<Row> dataset1 = dataset.filter("subject = 'Physics' and year>'2005'");
//		dataset1.show();
		
//		filter with lambda expression
//		Dataset<Row> dataset1=dataset.filter(row->row.getAs("subject").equals("Physics") && Integer.parseInt(row.getAs("year"))>2005);
//		dataset1.show();
		
		// columns references..
//		Column subjCol=functions.col("subject"); // col("subject")
//		Column yearCol=functions.col("year");     // col("year") in case of static import  
//		
//		dataset.filter(subjCol.equalTo("Chemistry").and(yearCol.gt(2005))).show();
//		
//		Column col1=col("subject").gt("Chemistry");
		
//		dataset.filter(col("subject").equalTo("Chemistry").and(col("Year").gt("2006"))).show();
		
		dataset.createOrReplaceTempView("student_view");
//		spark.sql("select * from student_view where subject='Chemistry' and year>=2005 " ).show();
//		spark.sql("select * from student_view where subject='Chemistry' and year>=2005 order by score desc limit 30" ).show();
		
		
		//inmemeory dataframe...
		
		List<Row> list=new ArrayList<>();
		
		list.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		list.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		list.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		list.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		list.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		list.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		StructField[] sf=new StructField[] {
				new StructField("Level",DataTypes.StringType,false,Metadata.empty()),
				new StructField("TimeStamp",DataTypes.StringType,false,Metadata.empty())
				
		};
		StructType structTypeObj=new StructType(sf);
		Dataset<Row> d1=spark.createDataFrame(list, structTypeObj);
		
//		d1.show();
		d1.createOrReplaceTempView("demo_view");
//		 spark.sql("select Level , collect_list(TimeStamp) from demo_view group by Level").show();
		 spark.sql("select Level , date_format(TimeStamp,'MMMM') as m,count(2) as c from demo_view group by 2,1 ").show();
		 
	
		//end of in memory object....
		
		
//		Scanner sc=new Scanner(System.in);
//		sc.nextLine();
		
		spark.close();
	}

}
