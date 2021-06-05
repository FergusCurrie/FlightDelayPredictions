package ONP;

// Think I need to add
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameNaFunctions;


import org.apache.spark.ml.feature.OneHotEncoderEstimator;
// Recommended by lecturer 
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.linalg.VectorUDT;

// For logistic regression
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel; 

// preparing data?
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StringIndexer;

// Basic imports java
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;

// x 

import org.apache.log4j.Logger;
import org.apache.log4j.Level;


// pipeline
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;


//pca
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;

// Linear Regression
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.evaluation.RegressionEvaluator;

public class OnlineNewsPopularity{
    

    public static Row run(int seed, Dataset<Row> data, Pipeline pipeline, String label_str){
        long startTime = System.currentTimeMillis();
        // Split into test and training
        Dataset<Row>[] split = data.randomSplit(new double[]{0.7,0.3},1232424); // 123 = seed 
        Dataset<Row> training = split[0];
        Dataset<Row> test = split[1];

        PipelineModel model = pipeline.fit(training);       
        LinearRegressionModel lrModel = (LinearRegressionModel) (model.stages()[model.stages().length - 1]);

        // Model 
        //System.out.println(lrModel.coefficients());
        //System.out.println(lrModel.intercept());


        // Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        Double train_rmse = trainingSummary.rootMeanSquaredError();
        Dataset<Row> predictions = model.transform(test);

        // Evaluate on test 
        RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol(label_str).setPredictionCol("prediction");
        Double test_rmse = evaluator.evaluate(predictions);
        long total_time = System.currentTimeMillis() - startTime;

        return RowFactory.create(train_rmse, test_rmse, total_time);
    }

















    public static void main(String[] args){
        // Initilisations 
        String f_path = "/user/currieferg/OnlineNewsPopInp/OnlineNewsPopularity.csv";
        Logger.getLogger("org").setLevel(Level.OFF);
        boolean do_norm = false; 
        boolean do_pca = false;
        String label_str = " shares";
        String app_name = "NewsPopularity";
	    SparkSession spark = SparkSession.builder().appName(app_name).master("local").getOrCreate();

        // Load and clean data. Then print the schema 
        Dataset<Row> data = spark.read().format("csv").option("sep", ",").option("nullfable","false").option("inferSchema", "true").option("header", "true").load(f_path);
        data = data.drop("url");
        data.printSchema();     
        
        // compile all features to single column
        System.out.println(data.columns());
        VectorAssembler assembler = new VectorAssembler()
            .setInputCols(Arrays.stream(data.columns()).filter(x -> !x.equals(label_str)).toArray(String[]::new))
            .setOutputCol("features");

        // Normalise features p1=l1 norm, p2=l2 norm
        // should this be same normalizer as model? 
        // this vs scaling? 
        Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("featuresNorm").setP(1.0);

        // https://datascience-enthusiast.com/Python/PCA_Spark_Python_R.html
        // i think have to normalise before pca rigth? 
        // k value  
        PCA pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(3); // K value ? 

        // Model
        //LogisticRegression lr = new LogisticRegression().setMaxIter(20).setRegParam(0.3).setLabelCol(label_str);
        LinearRegression lr = new LinearRegression().setMaxIter(20).setRegParam(0.3).setLabelCol(label_str).setFeaturesCol("features").setRegParam(3);//.setElasticNetParam(0.8);                          
        
        // Pipeline
        Pipeline pipeline = null;
        if(do_norm){
            pipeline = new Pipeline().setStages(new PipelineStage[] {assembler, normalizer, lr}); 
        }
        if(do_pca){
            // CURRENTLY ALWAYS NORMALISE BEFORE PCA 
            pipeline = new Pipeline().setStages(new PipelineStage[] {assembler, normalizer, pca, lr});
        }
        if(!do_norm && !do_pca){
            pipeline = new Pipeline().setStages(new PipelineStage[] {assembler, lr});
        }

        
        List<Row> results = new ArrayList<Row>();
        results.add(run(1231, data, pipeline, label_str));



        // Create row and return
        StructType schema_out = new StructType(new StructField[]{
            new StructField("TRAIN_RMSE", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("TEST_RMSE", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("TIME", DataTypes.LongType, false, Metadata.empty())
        });
        Dataset<Row> all_results = spark.createDataFrame(results, schema_out);

        all_results.write().csv("/user/currieferg/OnlineNewsPopRes/results");
        all_results.describe("TEST_RMSE").write().csv("/user/currieferg/OnlineNewsPopRes/summary");

    }
}
