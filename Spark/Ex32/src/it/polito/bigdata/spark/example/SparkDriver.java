package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		String startWord;
		
		inputPath=args[0];
		outputPath=args[1];


	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Ex32");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the input file  
		JavaRDD<String> recordsRDD = sc.textFile(inputPath);

		// Select only the pm10 value
		JavaRDD<Double> pm10RDD = recordsRDD.map((String record)->{
			String fields[] = record.split(",");
			return new Double(fields[2]);
		});
		
		//take the top 1 value
		List<Double> highestPM10RDD = pm10RDD.top(1);
		
		// Print on stdout
		for (Double value : highestPM10RDD) {
			System.out.println(value);
		}
		

		// Close the Spark context
		sc.close();
	}
}
