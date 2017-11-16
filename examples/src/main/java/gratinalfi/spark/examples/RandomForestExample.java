package gratinalfi.spark.examples;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.SparkSession;

public class RandomForestExample {

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
	    
	    // Initialize the hyperparameters for the RandomForest algorithm

	    /*
	    categoricalFeaturesInfo input is a Map storing arity of categorical features.
	    An Map entry (n -> k) indicates that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}.
	    */
	    Map<Integer,Integer> categoricalFeaturesInfo = new HashMap<>();  // all features are continuous.

	    // numTrees specifies number of trees in the random forest.
	    int numTrees = 3; // Use more in practice.
	    
	    /*
	    featureSubsetStrategy specifies number of features to consider for splits at each node.
	    MLlib supports: "auto", "all", "sqrt", "log2", "onethird".
	    If "auto" is specified, the algorithm choses a value based on numTrees: if numTrees == 1, featureSubsetStrategy is set to "all"; other it is set to "onethird".
	    */
	    String featureSubsetStrategy = "auto"; 

	    /*
	    impurity specifies the criterion used for information gain calculation.
	    Supported values: "variance"
	    */
	    String impurity = "variance";

	    /*
	    maxDepth specifies the maximum depth of the tree. Depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
	    Suggested value: 4
	    */
	    int maxDepth = 4;

	    /*
	    maxBins specifies the maximum number of bins to use for splitting features.
	    Suggested value: 100
	    */
	    int maxBins = 32;
	    
	    int seed = 3;
	    
	    // Train a model.
	    
	    RandomForestModel rfModel = RandomForest.trainRegressor(labeledPoints, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
	    
	}
}
