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
		String inputPath2;
		Double threshold;
		
		inputPath=args[0];
		inputPath2=args[1];
		outputPath=args[2];
		threshold= new Double(args[3]);
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Ex43");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the first input file (QUESTIONS)
		JavaRDD<String> occupancyRDD = sc.textFile(inputPath);
		
		//map to pair to create (sId, (1,1) ) or (sId , (0,1) )
		JavaPairRDD<String,Count> countRDD = occupancyRDD.mapToPair((line)->{
			String[] fields = line.split(",");
			Double nFree = new Double(fields[5]); 
			Count count;
			Tuple2<String,Count> sensorCount;
			
			if (nFree.compareTo(threshold) < 0) {
				//critical situation;
				count = new Count(1,1);
			}else {
				//Non critical
				count = new Count(0,1);
			}
			sensorCount = new Tuple2<String,Count>(fields[0],count);
			return sensorCount;
		});
		
		//reduce by key to sum critical situations and total
		JavaPairRDD<String,Count> sensorCountRDD = countRDD.reduceByKey((count1,count2)->{
			int total;
			int free;
			Count count;
			
			free = count1.getFreeSlots();
			free = free + count2.getFreeSlots();

			total = count1.getTotalSlots();
			total = total + count2.getTotalSlots();
			count = new Count(free,total);
			return count;
		});
		
		JavaPairRDD<Double,String> avgRDD = sensorCountRDD.mapToPair((pair)->{
			Tuple2<Double,String> avg = new Tuple2<Double,String>((double) (pair._2().getFreeSlots()/pair._2().getTotalSlots()),pair._1());
			return avg;
		});
		
		JavaPairRDD<Double,String> filteredAvgRDD = avgRDD.filter((pair)->{
			if (pair._1() > 0.8) {
				return true;
			}else {
				return false;
			}
		});
		
		JavaPairRDD<Double,String> sortedRDD = filteredAvgRDD.sortByKey();
		
		
		
		sortedRDD.saveAsTextFile(outputPath);


		// Close the Spark context
		sc.close();
	}
}

