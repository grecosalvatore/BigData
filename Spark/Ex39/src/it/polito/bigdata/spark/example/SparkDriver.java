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
		SparkConf conf=new SparkConf().setAppName("Spark Ex39");
		
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
		JavaPairRDD<String, String>  criticalSensorsRDD = filteredRecordsRDD.mapToPair((line)->{
			String[] fields = line.split(",");
			Tuple2<String,String> critical = new Tuple2<String,String>(new String(fields[0]),new String(fields[1]));
			return critical;
		});
		
		
		// group all dates by the key
		JavaPairRDD<String, Iterable<String>> criticalSensorsDatesRDD = criticalSensorsRDD.groupByKey();
		
		criticalSensorsDatesRDD.saveAsTextFile(outputPath);


		// Close the Spark context
		sc.close();
	}
}

