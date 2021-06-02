package FlightDelay;

// Think I need to add
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameNaFunctions;

// Recommended by lecturer 
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.linalg.Vectors;
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

// x 

import org.apache.log4j.Logger;
import org.apache.log4j.Level;


// pipeline
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;

public class FlightDelay{

    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.OFF);


        String label_str = "ARR_DEL15";
        String appName = "FlightDelay";
	    SparkSession spark = SparkSession.builder()
			.appName(appName)
			.master("local")
			.getOrCreate();

        Dataset<Row> data = spark.read().format("csv").option("sep", ",").option("nullfable","false").option("inferSchema", "true").option("header", "true").load("/user/currieferg/FlightDelayData/Jan_2020_ontime.csv");


        // ArrayList<String> drop_airport_info = new ArrayList<>(Arrays.asList("OP_CARRIER", "TAIL_NUM", "ORIGIN", "DEST", "DEP_TIME_BLK", "_c21", "OP_UNIQUE_CARRIER", "ARR_TIME"));
        ArrayList<String> drop_airport_info = new ArrayList<>(Arrays.asList("ORIGIN_AIRPORT_ID", "ORIGIN_AIRPORT_SEQ_ID", "ORIGIN", "DEST_AIRPORT_ID", "DEST_AIRPORT_SEQ_ID", "DEST")); 
        ArrayList<String> drop_plane_info = new ArrayList<>(Arrays.asList("OP_UNIQUE_CARRIER", "OP_CARRIER_AIRLINE_ID", "OP_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM"));
        ArrayList<String> drop_other = new ArrayList<>(Arrays.asList("DEP_TIME", "ARR_TIME", "_c21", "DEP_TIME_BLK"));
        for(String d : drop_airport_info){
            data = data.drop(d);
        }
        for(String d : drop_plane_info){
            data = data.drop(d);
        }
        for(String d : drop_other){
            data = data.drop(d);
        }


        //DataFrameNaFunctions dropper = new DataFrameNaFunctions();
        data = data.na().drop();

        // compile all features to single column
        VectorAssembler assembler = new VectorAssembler()
            .setInputCols(Arrays.stream(data.columns()).filter(x -> x!=label_str).toArray(String[]::new)) //x -> x!="label"
            .setOutputCol("features");
        //data = assembler.transform(data);
        


        data.printSchema();

        // Splitting data ... add stratified? 
        
        Dataset<Row>[] split = data.randomSplit(new double[]{0.7,0.3},123); // 123 = seed 
        Dataset<Row> training = split[0];
        Dataset<Row> test = split[1];


        // Model

        LogisticRegression lr = new LogisticRegression().setMaxIter(10) //Set maximum iterations
                                                        .setRegParam(0.3) //Set Lambda
                                                        .setElasticNetParam(0.8) //Set Alpha
                                                        .setLabelCol(label_str);    
        
        
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {assembler, lr});

        PipelineModel model = pipeline.fit(training);       

        Dataset<Row> predictions = model.transform(test);
        
    
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol(label_str)
            .setPredictionCol("prediction")
            .setMetricName("accuracy");
        

        double accuracy = evaluator.evaluate(predictions);

        List<Row> results = new ArrayList<Row>();
        results.add(RowFactory.create(accuracy));

        // creates output dataframe
        StructType schema_out = new StructType(new StructField[]{
            new StructField("test", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> all_results = spark.createDataFrame(results, schema_out);

        // write results to file
        all_results.write().csv("/user/currieferg/FlightDelayResult/results");
        
        // summary statistics
        all_results.describe("test").write().csv("/user/currieferg/FlightDelayResult/summary");







        

    }
}
