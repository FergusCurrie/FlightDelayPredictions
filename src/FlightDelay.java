package FlightDelay;

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

public class FlightDelay{
    


    /*
        VectorIndexerModel featureIndexer = new VectorIndexer()
            .setInputCol(Arrays.stream(data.columns()).filter(x -> x!=label_str).toArray(String[]::new))
            .setOutputCol("features")
            .setMaxCategories(20)
            .fit(data); // features with > 4 distinct values are treated as continuous(/)
            */









    /**
        Drop the data rows which are irrelevant 
     */
    public static Dataset<Row> do_drop(Dataset<Row> data){
        boolean dodrop = true;
        if(dodrop){
            ArrayList<String> drop_airport_info = new ArrayList<>(Arrays.asList("ORIGIN_AIRPORT_ID", "ORIGIN_AIRPORT_SEQ_ID", "ORIGIN", "DEST_AIRPORT_ID", "DEST_AIRPORT_SEQ_ID", "DEST")); 
            ArrayList<String> drop_plane_info = new ArrayList<>(Arrays.asList("OP_UNIQUE_CARRIER", "OP_CARRIER_AIRLINE_ID", "OP_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM"));
            
            for(String d : drop_airport_info){
                data = data.drop(d);
            }
            for(String d : drop_plane_info){
                data = data.drop(d);
            }
        }
        ArrayList<String> drop_other = new ArrayList<>(Arrays.asList("DEP_TIME", "ARR_TIME", "_c21")); // , "DEP_TIME_BLK"
        for(String d : drop_other){
             data = data.drop(d);
        }
        return data;
    }

    /**
        Convert DEP_TIME_BLK into one hot encoding
     */
    public static Dataset<Row> ohe_dep_time_blk(Dataset<Row> data){
        StringIndexer dtb_indexer = new StringIndexer().setInputCol("DEP_TIME_BLK").setOutputCol("DEP_TIME_BLK_INDEX");
        data = dtb_indexer.fit(data).transform(data);
        data = data.drop("DEP_TIME_BLK");
        String[] in = new String[1];
        String[] out = new String[1];
        in[0] = "DEP_TIME_BLK_INDEX";
        out[0] = "DEP_TIME_BLK_VEC";
        OneHotEncoderEstimator dtb_encoder = new OneHotEncoderEstimator().setInputCols(in).setOutputCols(out);
        data = dtb_encoder.fit(data).transform(data);
        data = data.drop("DEP_TIME_BLK_INDEX");
        return data;
    }

    public static Row one_run(int seed, Dataset<Row> data){
        
    }

    public static void main(String[] args){
        long startTime = System.currentTimeMillis();
        long totalTime = System.currentTimeMillis() - startTime;
        // Initilisations 
        Logger.getLogger("org").setLevel(Level.OFF);
        boolean do_norm = false; 
        boolean do_pca = false;
        String label_str = "ARR_DEL15";
        String app_name = "FlightDelay";
	    SparkSession spark = SparkSession.builder().appName(app_name).master("local").getOrCreate();

        // Load and clean data. Then print the schema 
        Dataset<Row> data = spark.read().format("csv").option("sep", ",").option("nullfable","false").option("inferSchema", "true").option("header", "true").load("/user/currieferg/FlightDelayData/Jan_2020_ontime.csv");
        data = do_drop(data);          // Drop irrelevant features 
        data = data.na().drop();       // Drop null values 
        data = ohe_dep_time_blk(data); // Convert DEP_TIME_BLK into one hot encoding
        data.printSchema();     
        
        // compile all features to single column
        VectorAssembler assembler = new VectorAssembler()
            .setInputCols(Arrays.stream(data.columns()).filter(x -> x!=label_str).toArray(String[]::new)) 
            .setOutputCol("features");

        // Normalise features p1=l1 norm, p2=l2 norm
        // should this be same normalizer as model? 
        // this vs scaling? 
        Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("featuresNorm").setP(1.0);

        // https://datascience-enthusiast.com/Python/PCA_Spark_Python_R.html
        // i think have to normalise before pca rigth? 
        // k value  
        PCA pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(3); // K value ? 

    

        // Split into test and training
        Dataset<Row>[] split = data.randomSplit(new double[]{0.7,0.3},1232424); // 123 = seed 
        Dataset<Row> training = split[0];
        Dataset<Row> test = split[1];


        // Model
        LogisticRegression lr = new LogisticRegression().setMaxIter(20).setRegParam(0.3).setLabelCol(label_str);
                                                
        
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


        PipelineModel model = pipeline.fit(training);       

        Dataset<Row> predictions = model.transform(test);
        
    

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol(label_str).setPredictionCol("prediction").setMetricName("accuracy");
        
        double accuracy = evaluator.evaluate(predictions);

        List<Row> results = new ArrayList<Row>();
        results.add(RowFactory.create(accuracy));

        // creates output dataframe
        StructType schema_out = new StructType(new StructField[]{
            new StructField("test", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> all_results = spark.createDataFrame(results, schema_out);
        
        LogisticRegressionModel lrModel = (LogisticRegressionModel) (model.stages()[model.stages().length-1]);
        System.out.println(lrModel.coefficientMatrix());
        System.out.println("###");
        System.out.println(lrModel.coefficientMatrix().toString());
        System.out.println("###");

        // write results to file
        //all_results.write().csv("/user/currieferg/FlightDelayResult/results");
        
        // summary statistics
        //all_results.describe("test").write().csv("/user/currieferg/FlightDelayResult/summary");

    }
}
