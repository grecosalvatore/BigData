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

		String inputPath1;	
		String inputPath2;
		String outputPath1;
		String outputPath2;
		Integer threshold;
		
		inputPath1=args[0];
		inputPath2=args[1];
		outputPath1=args[2];
		outputPath2=args[3];
		threshold = new Integer(args[4]);
		
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Ex43_MyVersion");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//PART -- A

		// Read the content of the first input file 
		JavaRDD<String> readingRDD = sc.textFile(inputPath1).cache();
		
		JavaPairRDD<String,CriticalTotal> crtiticalTotOneRDD = readingRDD.mapToPair(reading ->{
			String [] fields = reading.split(",");
			String stationId = fields[0];
			Integer freeSlots = new Integer(fields[5]);
			CriticalTotal ct;
			Tuple2<String,CriticalTotal>pair;
			
			if (freeSlots < threshold) 
						//critical
						ct = new CriticalTotal(1,1);
			else
						//non critical
						ct = new CriticalTotal(0,1);
			pair = new Tuple2<String,CriticalTotal>(stationId,ct);
			return pair;
		});
		
		JavaPairRDD<String,CriticalTotal> sumCriticalRDD = crtiticalTotOneRDD.reduceByKey((v1,v2)-> {
			int critical = 0;
			int total = 0;
			critical = v1.getCritical() + v2.getCritical();
			total = v1.getTotal() + v2.getTotal();
			return new CriticalTotal(critical,total);
		});
		
		JavaPairRDD<Double,String> percentageRDD = sumCriticalRDD.mapToPair((Tuple2<String,CriticalTotal>pair)->{
			Double percentage;
			percentage = 100 * (double)pair._2().getCritical() / (double)pair._2().getTotal();
			Tuple2<Double,String>pairOut = new Tuple2<Double,String>(percentage,pair._1());
			return pairOut;
		});
		
		
		JavaPairRDD<Double,String> filterPercentageRDD = percentageRDD.filter((Tuple2<Double,String>pair)->{
			if (pair._1()>10)
				return true;
			else
				return false;
		});
		
		JavaPairRDD<Double,String> sortedPercentageRDD = filterPercentageRDD.sortByKey();
		
		sortedPercentageRDD.saveAsTextFile(outputPath1);

		//PART -- B
		
		JavaPairRDD<String,CriticalTotal> timeslotRDD = readingRDD.mapToPair(reading->{
			String [] fields = reading.split(",");
			Integer Hour = new Integer(fields[2]);
			String stationId = fields[0];
			Integer freeSlots = new Integer(fields[5]);
			CriticalTotal ct;
			Tuple2<String,CriticalTotal>pair;
			
			if (freeSlots < threshold) 
						//critical
						ct = new CriticalTotal(1,1);
			else
						//non critical
						ct = new CriticalTotal(0,1);
			pair = new Tuple2<String,CriticalTotal>("[" + ((Hour/4)*4) + "-" + ((Hour/4)*4+3) + "] , " + stationId,ct);
			return pair;
		});
		

		JavaPairRDD<String,CriticalTotal> sumTimeslotCriticalRDD = timeslotRDD.reduceByKey((v1,v2)-> {
			int critical = 0;
			int total = 0;
			critical = v1.getCritical() + v2.getCritical();
			total = v1.getTotal() + v2.getTotal();
			return new CriticalTotal(critical,total);
		});
		
		JavaPairRDD<Double,String> percentageTimeslotRDD = sumTimeslotCriticalRDD.mapToPair((Tuple2<String,CriticalTotal>pair)->{
			Double percentage;
			percentage = 100 * (double)pair._2().getCritical() / (double)pair._2().getTotal();
			Tuple2<Double,String>pairOut = new Tuple2<Double,String>(percentage,pair._1());
			return pairOut;
		});
		
		
		JavaPairRDD<Double,String> filtPercentageTimeslotRDD = percentageTimeslotRDD.filter((Tuple2<Double,String>pair)->{
			if (pair._1()>10)
				return true;
			else
				return false;
		});
		
		JavaPairRDD<Double,String> sortedPercentageTimeslotRDD = filtPercentageTimeslotRDD.sortByKey();
		
		sortedPercentageTimeslotRDD.saveAsTextFile(outputPath2);
		
		
		//PART -- C

		JavaRDD<String> fullStationsRDD = readingRDD.filter(reading -> {
			String [] fields = reading.split(",");
			Integer freeSlots = new Integer (fields[5]);
			if (freeSlots == 0) 
				return true;
			else
				return false;
		});
		
		// Read the content of the first second file 
		JavaRDD<String> neighborsRDD = sc.textFile(inputPath2);
		
		// Close the Spark context
		sc.close();
	}
}

