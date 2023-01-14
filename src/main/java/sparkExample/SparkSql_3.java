package sparkExample;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class SparkSql_3 {
	
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("SparkSql_3").master("local[*]").getOrCreate();
		
		Dataset<Row> dataset=spark.read().option("header",true).csv("src/main/resources/biglog.txt");
//		dataset.select(functions.col("level"),functions.date_format(functions.col("datetime"),"MMMM").alias("month"))
//		.show();
		dataset=dataset.select(functions.col("level"),functions.date_format(functions.col("datetime"),"MMMM").alias("Month"),
		functions.date_format(functions.col("datetime"),"M").alias("MonthNo").cast(DataTypes.IntegerType));
		
//		dataset.groupBy(functions.col("level"),functions.col("Month"),functions.col("MonthNo"))
//		dataset.groupBy(functions.col("level"),functions.col("Month"),functions.col("MonthNo"))
//		.count()
//		.orderBy("MonthNo")
//		.show();
		
//		dataset.groupBy(functions.col("level")).pivot("Month", Arrays.asList(new Object[] {"1","2","3","4","5"})).count().show();
		dataset.groupBy(functions.col("level")).pivot("Month").count().show();
		
//		dataset.show();
		
		
		
		spark.close();
		
	}

}
