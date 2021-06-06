package ONP;

// Think I need to add
import org.apache.spark.sql.SparkSession;


// Recommended by lecturer 
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.feature.Normalizer;


// preparing data?
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StringIndexer;

// Basic imports java
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.List;


import org.apache.spark.api.java.JavaRDD;

// x 
import org.apache.spark.ml.feature.StandardScaler;
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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
// Linear Regression
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.evaluation.RegressionEvaluator;

import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel;
import java.lang.Math;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import java.util.function.Function;
import org.apache.spark.sql.SQLContext;


public class OnlineNewsPopularity{
    

    public static Row run(int seed, Dataset<Row> data, Pipeline pipeline, String label_str){
        long startTime = System.currentTimeMillis();
        // Split into test and training
        Dataset<Row>[] split = data.randomSplit(new double[]{0.7,0.3}, seed); // 123 = seed 
        Dataset<Row> training = split[0];
        Dataset<Row> test = split[1];

        PipelineModel model = pipeline.fit(training);       
        LinearRegressionModel lrModel = (LinearRegressionModel) (model.stages()[model.stages().length - 1]);
        //GeneralizedLinearRegressionModel lrModel = (GeneralizedLinearRegressionModel) (model.stages()[model.stages().length - 1]);

        // Model 
        System.out.println(lrModel.coefficients());
        System.out.println(lrModel.intercept());

        // Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        Double train_rmse = trainingSummary.rootMeanSquaredError();
        //Double train_rmse = 0.0;
        Dataset<Row> predictions = model.transform(test);

        // Evaluate on test 
        RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol(label_str).setPredictionCol("prediction");
        Double test_rmse = evaluator.evaluate(predictions);
        long total_time = System.currentTimeMillis() - startTime;
        //System.out.println(test_rmse);
        //System.out.println(lrModel.numFeatures());
       

        //PCAModel pcaModel = (PCAModel) (model.stages()[model.stages().length - 2]);
        //System.out.println(pcaModel.explainedVariance());
        return RowFactory.create(train_rmse, test_rmse, total_time, seed);
    }


    public static void main(String[] args){
        // Initilisations 
        String f_path = "/user/currieferg/OnlineNewsPopInp/ln_OnlineNewsPopularity.csv";
        Logger.getLogger("org").setLevel(Level.OFF);
        boolean do_scale = true; 
        boolean do_pca = true;
        String label_str = " nl_shares";
        String app_name = "NewsPopularity";
	    SparkSession spark = SparkSession.builder().appName(app_name).getOrCreate(); //.master("local") .master("local")

        // Load and clean data.  
        Dataset<Row> data = spark.read().format("csv").option("sep", ",").option("nullfable","false").option("inferSchema", "true").option("header", "true").load(f_path);
        List<String> to_drop = Arrays.asList("url", " shares", " timedelta"," kw_min_min"," kw_avg_min"," kw_min_avg");
        //List<String> to_drop = Arrays.asList("url");
        for(int i = 0; i < to_drop.size(); i++){
            data = data.drop(to_drop.get(i)); 
        }
        data.printSchema();
        /*
        Trying to convert myself
        data.printSchema();

        Dataset<Row> shares = data.select(" shares");
        JavaRDD<Double> shares_d = shares.toJavaRDD().map(row -> row.getDouble(0));
        JavaRDD<Double> log_shares = shares_d.map(s -> Math.log(s));

        StructType log_shares_struct = new StructType(new StructField[]{
            new StructField("log_shares", DataTypes.DoubleType, false, Metadata.empty())
        });
        
        Dataset<Row> log_shares_ds = spark.createDataFrame(log_shares, JavaRDD<Double>);//log_shares_struct.getClass()); //
        log_shares_ds.printSchema();
        data = data.join(log_shares_ds);
         data.printSchema();
        System.out.println("1");
        */



        // Pipeline Stages 
        VectorAssembler assembler = new VectorAssembler().setInputCols(Arrays.stream(data.columns()).filter(x -> !x.equals(label_str)).toArray(String[]::new)).setOutputCol("features");
        StandardScaler scaler  = new StandardScaler().setInputCol("features").setOutputCol("features_scale").setWithStd(true).setWithMean(true);
        PCA pca = new PCA().setInputCol("features_scale").setOutputCol("features_pca").setK(3); // K value ? 
        
        
        String features_col = "features";
        if(do_scale){
            features_col = "features_scale";
        }
        if(do_pca){
            features_col = "features_pca";
        }

        LinearRegression lr = new LinearRegression().setMaxIter(20).setLabelCol(label_str).setFeaturesCol(features_col).setFitIntercept(true).setElasticNetParam(0.8);//.setElasticNetParam(0.8);



        // Pipeline
        Pipeline pipeline = null;
        if(do_scale){
            pipeline = new Pipeline().setStages(new PipelineStage[] {assembler, scaler, lr}); 
        }
        if(do_pca){
            pipeline = new Pipeline().setStages(new PipelineStage[] {assembler, scaler, pca, lr}); // Always standardise before pca 
        }
        if(!do_scale && !do_pca){
            pipeline = new Pipeline().setStages(new PipelineStage[] {assembler, lr});
        }

        // Run 10 times with 10 seeds 
        List<Row> results = new ArrayList<Row>();
        for(int i = 0; i < 10; i++){
            results.add(run(i, data, pipeline, label_str));
        }

        // Saving results 
        StructType schema_out = new StructType(new StructField[]{
            new StructField("TRAIN_RMSE", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("TEST_RMSE", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("TIME", DataTypes.LongType, false, Metadata.empty()),
            new StructField("SEED", DataTypes.IntegerType, false, Metadata.empty())
        });
        Dataset<Row> all_results = spark.createDataFrame(results, schema_out);
        all_results.write().csv("/user/currieferg/OnlineNewsPopRes/results");
        all_results.describe("TEST_RMSE").write().csv("/user/currieferg/OnlineNewsPopRes/summary");
    }
}
