package sparkExample;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class SparkSql_4 {
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("SparkSql_3").master("local[*]").getOrCreate();
Map<String, String> options=new HashMap();
options.put("header","true");
options.put("inferSchema","true");
		Dataset<Row> dataset = spark.read()
//				.option("header", true)
				.options(options)
				.format("csv")
				.load("src/main/resources/students.csv");
//				.csv("src/main/resources/students.csv");
//		dataset.createOrReplaceTempView("student");
		
//		spark.sql("select subject,max(score)as s  from student group by subject order by s " ).show();
		dataset.select(functions.col("student_id"),functions.col("exam_center_id").cast(DataTypes.DoubleType),
				functions.col("subject").as("Subject_Name"),
				functions.col("Score"),
				functions.col("grade")).show();
//		.groupBy("Score").pivot("grade")
//		.count().show();
//		dataset.show();
		
		spark.close();
		
		
//		dataset.select(functions.col("student_id"),functions.col("subject"),functions.col("year"),
//				functions.col("score").cast(DataTypes.IntegerType),functions.col("grade"))
//		.groupBy(functions.col("subject")).max("score")
//		.show();
//		dataset.show();
		
		
	}

}
