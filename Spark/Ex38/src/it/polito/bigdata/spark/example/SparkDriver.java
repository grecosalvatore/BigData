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
		SparkConf conf=new SparkConf().setAppName("Spark Ex38");
		
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
		
		
		// Reduce by key sum the ones
		JavaPairRDD<String, Double> criticalSensorsSumRDD = criticalSensorsRDD.reduceByKey((value1,value2)->{
			Double sum = new Double(value1+value2);
			return sum;
		});
		
		//filter only sensor with at least 2 count
		JavaPairRDD<String, Double> filteredCriticalSensorsSumRDD = criticalSensorsSumRDD.filter((pair)->{
			if (pair._2()>= 2) {
				return true;
			}else {
				return false;
			}
		});
		
		filteredCriticalSensorsSumRDD.saveAsTextFile(outputPath);


		// Close the Spark context
		sc.close();
	}
}

