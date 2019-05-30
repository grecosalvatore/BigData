package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		
		
		inputPath=args[0];
		outputPath=args[1];


	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Ex36");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		JavaRDD<String> recordRDD = sc.textFile(inputPath);

		// select only pm10 value
		JavaRDD<Double> pm10RDD = recordRDD.map((String line) -> {
			String[] fields = line.split(",");
			return new Double(fields[2]);
		});
		
		// reduce taking sum
		Double sumPM10 = pm10RDD.reduce((Double value1,Double value2)->{
			return (value1 + value2);
		});
		
		// iterate multiple time -> CACHE
		Long nElements = recordRDD.count();	
		
		// Print the avg on stdout
		System.out.println("Avg = " + (sumPM10/nElements));
		
		Float tmpAvg = new Float(sumPM10/nElements);
		
		List<Float> avg = Arrays.asList(tmpAvg);
		
		JavaRDD<Float> avgRDD = sc.parallelize(avg);
		
		// Store the result in the output folder
		avgRDD.saveAsTextFile(outputPath);


		// Close the Spark context
		sc.close();
	}
}