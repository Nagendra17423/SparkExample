package sparkExample;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql_2 {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		System.setProperty("hadoop.home.dir", "D:/Spark/Hadoop");
		

		SparkSession spark = SparkSession.builder().appName("SparkSql2").master("local[*]").getOrCreate();

		Dataset<Row> r1 = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

		r1.createOrReplaceTempView("log_view");

		Dataset<Row> r2 = spark.sql(
				"select level,date_format(datetime,'MMMM') as month1,"
				+ "cast(first(date_format(datetime,'M')) as int) as monthcount ,count(*) from log_view group by 2,1 order by monthcount,level");

		
		r2.show();

		spark.close();
	}

}
