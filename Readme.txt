
Make input dir:
% hdfs dfs -mkdir /user/currieferg/FlightDelayData

Make output dir:
% hdfs dfs -mkdir /user/currieferg/FlightDelayResult

Move data file to hadoop:
% hdfs dfs -put Jan_2020_ontime.csv FlightDelayData

View data in hadoop:
% hdfs dfs -ls Jan_2020_ontime.csv FlightDelayData





Lecture Imports:

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorsUDT;


Old DecTre;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StringIndexer;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.spark.sql.RowFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;







Features : 

DAY_OF_MONTH, DAY_OF_WEEK, OP_UNIQUE_CARRIER, OP_CARRIER_AIRLINE_ID, 
OP_CARRIER, TAIL_NUM, OP_CARRIER_FL_NUM, ORIGIN_AIRPORT_ID, ORIGIN_AIRPORT_SEQ_ID, 
ORIGIN, DEST_AIRPORT_ID, DEST_AIRPORT_SEQ_ID, DEST, DEP_TIME, DEP_DEL15, DEP_TIME_BLK, 
ARR_TIME, ARR_DEL15, CANCELLED, DIVERTED, DISTANCE, _c21
