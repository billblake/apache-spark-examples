package gratinalfi.spark.examples;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.rdd.DoubleRDDFunctions;
import org.apache.spark.rdd.RDD;

public class LinearRegressionExample {

private static final String logFile = "/Applications/Spark/spark-2.2.0-bin-hadoop2.7/data/mllib/ridge-data/lpsa.data";
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf().setAppName("My Spark SQL app");
	    SparkContext sc = new SparkContext(config);
	    SparkSession sparkSession = SparkSession.builder().config(config).getOrCreate();
	    
	    JavaRDD<String> lines = sc.textFile(logFile, 2).toJavaRDD();
	    JavaRDD<LabeledPoint> labeledPoints = lines.map(row -> {
	    			String[] labelAndFeatures = row.split(",");
	    			Double label = Double.parseDouble(labelAndFeatures[0]);
	    			double[] features = Arrays.stream(labelAndFeatures[1].split(" "))
	    					.mapToDouble(d -> Double.parseDouble(d)).toArray();
	    			return new LabeledPoint(label, Vectors.dense(features));
	    	});
	    
	    labeledPoints.cache();
	    
	    int numIterations = 100;

		// train a model
    		LinearRegressionModel linearRegressionModel = LinearRegressionWithSGD.train(labeledPoints.rdd(), numIterations);
    		System.out.println("Intercept: " + linearRegressionModel.intercept());
    		System.out.println("Weights: " + linearRegressionModel.weights());
    		
    		JavaRDD<Tuple2<Object, Object>> observedAndPredictedLabels = labeledPoints.map(observation -> {
    			double predictedLabel = linearRegressionModel.predict(observation.features());
    			return new Tuple2<Object, Object>(observation.label(), predictedLabel);
    		});
    		
    		for (Tuple2 tuple : observedAndPredictedLabels.collect()) {
    			System.out.println(String.format("Expected = %s, Predicted = %s", tuple._1, tuple._2));
    		}
    		
    		// calculate square of difference between predicted and actual label for each observation
    		JavaRDD<Double> squaredErrors = observedAndPredictedLabels.map(observedAndPredicted -> {
    			return Math.pow(((Double) observedAndPredicted._1 - (Double) observedAndPredicted._2), 2);
    		});
    		
    		RDD<Object> rdd = JavaRDD.toRDD(squaredErrors.map(entry -> {
    			return (Object) entry;
    		}));
    		DoubleRDDFunctions rddFunctions = new DoubleRDDFunctions(rdd);
    		System.out.println("Mean: " + rddFunctions.mean());
    		
    		// Create an instance of the RegressionMetrics class
    		RegressionMetrics regressionMetrics = new RegressionMetrics(JavaRDD.toRDD(observedAndPredictedLabels));

    		// Check the various evaluation metrics
    		 System.out.println("Mean Squared Error: " + regressionMetrics.meanSquaredError());
	}
}
