package gratinalfi.spark.examples;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MysqlExample2 implements Serializable {

	private static final long serialVersionUID = -7822436421783167880L;

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(MysqlExample2.class);

	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/billTest";
	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "password";

	public static void main(String[] args) throws AnalysisException {
		SparkConf config = new SparkConf().setAppName("My Spark SQL app");
		SparkSession sparkSession = SparkSession.builder().config(config).getOrCreate();

		Dataset<Row> dataframe_mysql = sparkSession.read()
					.format("jdbc")
					.option("url", MYSQL_CONNECTION_URL)
					.option("driver", MYSQL_DRIVER)
					.option("dbtable", "person")
					.option("user", MYSQL_USERNAME)
					.option("password", MYSQL_PWD)
					.load();
		 
		dataframe_mysql.show();
		
		
		dataframe_mysql.createOrReplaceTempView("person");
		
		List<Row> rows = dataframe_mysql.sqlContext().sql("select * from person").collectAsList();
		for (Row row : rows) {
			System.out.println("****** " + row.get(0) + " " + row.get(1) + " " + row.get(2));
		}
	}
}
