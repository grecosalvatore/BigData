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
		
		
		inputPath=args[0];
		outputPath=args[1];


	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Ex37");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> recordRDD = sc.textFile(inputPath);

		// use map to paur <sensorId,[om10values]>
		JavaPairRDD<String, Double> sensorsPM10ValuesRDD = recordRDD.mapToPair(( line) -> {
			String[] fields = line.split(",");
			Tuple2<String,Double> pair = new Tuple2<String,Double>(new String(fields[0]),new Double(fields[2]));
			return pair;
		});
		
		// reduce taking the highest
		JavaPairRDD<String, Double>  maxSensorsPM10ValuesRDD = sensorsPM10ValuesRDD.reduceByKey((value1,value2)->{
			if (value1.compareTo(value2) > 0) {
				return value1;
			}else {
				return value2;
			}
		});
		
		
		// Store the result in the output folder
		maxSensorsPM10ValuesRDD.saveAsTextFile(outputPath);


		// Close the Spark context
		sc.close();
	}
}

