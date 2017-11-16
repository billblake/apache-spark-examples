package gratinalfi.spark.examples;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 
 * Reads in a file, stores it in a dataframe and then runs queries on it
 *
 */
public class SparkSqlExample {

	private static final String logFile = "/Applications/Spark/spark-2.2.0-bin-hadoop2.7/examples/src/main/resources/employees.csv";

	public static void main(String[] args) {
		SparkConf config = new SparkConf().setAppName("My Spark SQL app");
		SparkContext sc = new SparkContext(config);
		SparkSession sparkSession = SparkSession.builder().config(config).getOrCreate();

		JavaRDD<String> lines = sc.textFile(logFile, 2).toJavaRDD();
		;
		JavaRDD<String[]> linesSplit = lines.map(row -> row.split(","));
		JavaRDD<Employee> employees = linesSplit
				.map(cols -> new Employee(cols[0], Integer.parseInt(cols[1].trim()), cols[2]));

		Dataset<Row> employeesDf = sparkSession.createDataFrame(employees, Employee.class);
		employeesDf.cache();
		System.out.println("***Employees***");
		employeesDf.show();
		employeesDf.createOrReplaceTempView("employeeView");

		Dataset<Row> count = sparkSession.sql("SELECT count(*) count from employeeView");
		Row[] dataRows = (Row[]) count.collect();
		System.out.println("Count: " + dataRows[0].get(0));
		count.show();

		Dataset<Row> males = sparkSession.sql("SELECT name, age, gender from employeeView where gender == 'Male'");
		for (Row row : males.collectAsList()) {
			System.out.println("Name: " + row.get(0));
			System.out.println("Age: " + row.getAs("age"));
			System.out.println("Gender: " + row.get(row.fieldIndex("gender")));
			System.out.println("Name2: " + row.get(0));
			System.out.println();
		}

		Dataset<Row> aggs = sparkSession.sql("SELECT count(name), max(age), min(age) from employeeView");
		for (Row row : aggs.collectAsList()) {
			System.out.println("count(name): " + row.get(0));
			System.out.println("max(age): " + row.get(1));
			System.out.println("min(age): " + row.get(2));
			System.out.println();
		}

		Dataset<Row> filtered = employeesDf.filter("name='Bill'");
		filtered.show();

		Dataset<Row> countByGender = employeesDf.groupBy("gender").count();
		countByGender.show();

		List<Employee> employee2 = Arrays.asList(new Employee("Fred", 37, "Male"), new Employee("Bill", 37, "Male"),
				new Employee("Rob", 45, "Male"), new Employee("Abby", 52, "Female"),
				new Employee("Fiona", 23, "Female"));
		Dataset<Employee> employee2Df = sparkSession.createDataset(employee2, Encoders.bean(Employee.class));

		employeesDf.intersect(employee2Df.toDF()).show();

		List<User> users = Arrays.asList(new User("Fred", "UK"), new User("Bill", "Irish"),
				new User("Clare", "Aussie"));
		Dataset<User> usersDf = sparkSession.createDataset(users, Encoders.bean(User.class));
		usersDf.cache();

		// employeesDf.join(usersDf, usersDf.col("name")).show();
		employeesDf.crossJoin(employee2Df.toDF()).show();

		List joinColList = Arrays.asList("name");
		employeesDf.join(usersDf, scala.collection.JavaConversions.asScalaBuffer(joinColList), "leftouter").show();
		employeesDf.join(usersDf, scala.collection.JavaConversions.asScalaBuffer(joinColList), "left").show();
		employeesDf.join(usersDf, scala.collection.JavaConversions.asScalaBuffer(joinColList), "right").show();

		Dataset inner = employeesDf.join(usersDf, scala.collection.JavaConversions.asScalaBuffer(joinColList), "inner");
		inner.show();
		inner.write().format("org.apache.spark.sql.json").partitionBy("name").save("out2.json");

	}

	public static class Employee implements Serializable {
		private String name;
		private int age;
		private String gender;

		public Employee(String name, int age, String gender) {
			super();
			this.name = name;
			this.age = age;
			this.gender = gender;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}

	public static class User implements Serializable {
		private String name;
		private String country;

		public User(String name, String country) {
			super();
			this.name = name;
			this.country = country;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}
	}
}
