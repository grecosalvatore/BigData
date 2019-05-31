package it.polito.bigdata.spark.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		Double threshold;
		
		inputPath=args[0];
		outputPath=args[1];
		threshold=new Double(args[2]);
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Ex40");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> recordRDD = sc.textFile(inputPath);

		// filter taking only lines with pm10 greater then threshold
		JavaRDD<String> filteredRecordsRDD = recordRDD.filter(( line) -> {
			String[] fields = line.split(",");
			Double pm10 = new Double(fields[2]);
			if ((pm10.compareTo(threshold)) > 0) {
				return true;
			}else {
				return false;
			}
		});
		
		// map to pair (SensorId,1)
		JavaPairRDD<String, Double>  criticalSensorsRDD = filteredRecordsRDD.mapToPair((line)->{
			String[] fields = line.split(",");
			Tuple2<String,Double> critical = new Tuple2<String,Double>(new String(fields[0]),new Double(1));
			return critical;
		});
		
		
		// reduce by key to sum the ones
		JavaPairRDD<String, Double> sumCriticalSensorsRDD = criticalSensorsRDD.reduceByKey((value1,value2)->{
			return new Double(value1+value2);
		});
		
		JavaPairRDD<Double,String> invertedRDD = sumCriticalSensorsRDD.mapToPair((pair)-> new Tuple2<Double,String>(pair._2(),pair._1()));
		
		JavaPairRDD<Double,String> orderedCriticalSensorsRDD = invertedRDD.sortByKey(false);
		
		orderedCriticalSensorsRDD.saveAsTextFile(outputPath);


		// Close the Spark context
		sc.close();
	}
}

