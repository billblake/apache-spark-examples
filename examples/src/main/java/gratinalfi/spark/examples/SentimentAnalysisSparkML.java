package gratinalfi.spark.examples;
import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

/**
 * 
 * Sentiment Analysis Example using Spark ML
 *
 */
public class SentimentAnalysisSparkML implements Serializable {

	private static final long serialVersionUID = 8145811604846168100L;

	private static final String logFile = "/Users/bill/Documents/code/new/apacheSpark/my-app/sentiment_labelled_sentences/imdb_labelled.txt";

	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf().setAppName("My Spark SQL app");
		SparkContext sc = new SparkContext(config);
		SparkSession sparkSession = SparkSession.builder().config(config).getOrCreate();

		JavaRDD<String> lines = sc.textFile(logFile, 2).toJavaRDD();
		lines.persist(StorageLevel.MEMORY_ONLY());

		JavaRDD<String[]> columns = lines.map(line -> {
			return line.split("\\t");
		});

		SentimentAnalysisSparkML sentimentAnalysisSparkML = new SentimentAnalysisSparkML();

		JavaRDD<Review> reviewsRdd = columns.map(a -> {
			return sentimentAnalysisSparkML.new Review(a[0], Double.parseDouble(a[1]));
		});

		System.out.println("****** " + reviewsRdd.count());

		Dataset<Row> reviews = sparkSession.createDataFrame(reviewsRdd, Review.class);

		reviews.printSchema();

		reviews.groupBy("label").count().show();

		double[] splits = { 0.8, 0.2 };
		Dataset<Row>[] trainingAndTest = reviews.randomSplit(splits);

		Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");

		Dataset<Row> tokenizedData = tokenizer.transform(trainingAndTest[0]);
		// Row firstRow = tokenizedData.first();
		// System.out.println(String.format("**** <%s> <%s> <%s>", firstRow.get(0),
		// firstRow.get(1), firstRow.get(2)));
		//
		HashingTF hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol())
				.setOutputCol("features");

		Dataset<Row> hashedData = hashingTF.transform(tokenizedData);
		Row firstRow = hashedData.first();
		System.out.println(String.format("**** <%s> <%s> <%s> <%s>", firstRow.get(0), firstRow.get(1), firstRow.get(2),
				firstRow.get(3)));

		//
		LogisticRegression lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01);

		PipelineStage[] pipeLineStages = { tokenizer, hashingTF, lr };

		Pipeline pipeline = new Pipeline().setStages(pipeLineStages);
		//
		PipelineModel pipeLineModel = pipeline.fit(trainingAndTest[0]);

		Dataset<Row> testPredictions = pipeLineModel.transform(trainingAndTest[1]);

		Dataset<Row> trainingPredictions = pipeLineModel.transform(trainingAndTest[0]);

		BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator();

		Param<String> metricName = evaluator.metricName();

		ParamMap evaluatorParamMap = new ParamMap();
		double aucTraining = evaluator.evaluate(trainingPredictions, evaluatorParamMap);
		System.out.println();
		System.out.println("**** aucTraining " + aucTraining);

		double aucTest = evaluator.evaluate(testPredictions, evaluatorParamMap);
		System.out.println();
		System.out.println("**** aucTest " + aucTest);

		int[] numFeatures = { 10000, 100000 };
		double[] regParam = { 0.01, 0.1, 1.0 };
		int[] maxIter = { 20, 30 };

		ParamMap[] paramGrid = new ParamGridBuilder().addGrid(hashingTF.numFeatures(), numFeatures)
				.addGrid(lr.regParam(), regParam).addGrid(lr.maxIter(), maxIter).build();

		CrossValidator crossValidator = new CrossValidator().setEstimator(pipeline).setEstimatorParamMaps(paramGrid)
				.setNumFolds(10).setEvaluator(evaluator);

		CrossValidatorModel crossValidatorModel = crossValidator.fit(trainingAndTest[0]);

		Dataset<Row> newPredictions = crossValidatorModel.transform(trainingAndTest[1]);

		double newAucTest = evaluator.evaluate(newPredictions, evaluatorParamMap);
		System.out.println();
		System.out.println("newAucTest " + newAucTest);

		Model<?> bestModel = crossValidatorModel.bestModel();

		Dataset<Row> newPredictions2 = bestModel.transform(trainingAndTest[1]);
		double newAucTest2 = evaluator.evaluate(newPredictions2, evaluatorParamMap);
		System.out.println();
		System.out.println("newAucTest2 " + newAucTest2);

		((PipelineModel) bestModel).save("/Users/bill/Documents/code/new/apacheSpark/my-app/model");
	}

	public class Review implements Serializable {

		private static final long serialVersionUID = 1L;
		private String text;
		private Double label;

		public Review(String text, Double label) {
			this.text = text;
			this.label = label;
		}

		public String getText() {
			return text;
		}

		public void setText(String text) {
			this.text = text;
		}

		public Double getLabel() {
			return label;
		}

		public void setLabel(Double label) {
			this.label = label;
		}

	}
}
