package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		String startWord;
		
		inputPath=args[0];
		outputPath=args[1];


	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Ex34");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		JavaRDD<String> recordRDD = sc.textFile(inputPath).cache();

		// select only pm10 value
		JavaRDD<Double> pm10RDD = recordRDD.map((String line) -> {
			String[] fields = line.split(",");
			return new Double(fields[2]);
		});
		
		// reduce taking only the highest value
		Double highPM10 = pm10RDD.reduce((Double value1,Double value2)->{
			if (value1 > value2) {
				return value1;
		}else {
			return value2;
		}});
		
		// iterate multiple time -> CACHE
		JavaRDD<String> selectedRDD = recordRDD.filter((record)->{
			String[] fields = record.split(",");
			Double PM10 = new Double(fields[2]);
			if (PM10 == highPM10) {
				return true;
			}else {
				return false;
			}	
		});
				
		
		// Store the result in the output folder
		selectedRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
