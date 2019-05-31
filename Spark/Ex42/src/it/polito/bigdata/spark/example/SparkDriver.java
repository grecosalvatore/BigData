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
		
		inputPath=args[0];
		inputPath2=args[1];
		outputPath=args[2];
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Ex40");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the first input file (QUESTIONS)
		JavaRDD<String> questionRDD = sc.textFile(inputPath);
		
		// Read the content of the second input file (ANSWERS)
		JavaRDD<String> answerRDD = sc.textFile(inputPath2);
		
		//mapToPair of questions -> <QId,textQuestion>
		JavaPairRDD<String,String> questionPairRDD = questionRDD.mapToPair((record)->{
			String[] fields = record.split(",");
			Tuple2<String,String> pair = new Tuple2<String,String>(fields[0],fields[2]);
			return pair;
		});
		
		//mapToPair answers -> <QId,textAnswer>
		JavaPairRDD<String,String> answerPairRdd = answerRDD.mapToPair((record)->{
			String[] fields = record.split(",");
			Tuple2<String,String> pair = new Tuple2<String,String>(fields[1],fields[3]);
			return pair;
		});
		
		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> mappingRDD = questionPairRDD.cogroup(answerPairRdd);
		
		mappingRDD.saveAsTextFile(outputPath);


		// Close the Spark context
		sc.close();
	}
}

