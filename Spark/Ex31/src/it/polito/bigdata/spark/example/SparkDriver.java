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
		SparkConf conf=new SparkConf().setAppName("Spark Ex31");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the input file  
		JavaRDD<String> logRDD = sc.textFile(inputPath);

		// Filter the rows that contains www.google.com
		JavaRDD<String> resultRDD = logRDD.filter((String logRecord) -> {
			if (logRecord.contains("www.google.com")) {
				return true;
			}else {
				return false;
			}
		});
		
		// Map the rows taking only the ip
		JavaRDD<String> ipRDD = resultRDD.map((String lineFiltered)->{
			String[] fields = lineFiltered.split("--");
			return fields[0];
		});
		
		// Removes duplicates ip
		JavaRDD<String> distinctIpRDD = ipRDD.distinct();
		
		
		// Store the result in the output folder
		distinctIpRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
