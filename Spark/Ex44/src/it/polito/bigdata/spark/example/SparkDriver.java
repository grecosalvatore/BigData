package it.polito.bigdata.spark.example;

import java.util.ArrayList;
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
		String inputPath3;
		Double threshold;
		
		inputPath=args[0];
		inputPath2=args[1];
		inputPath3=args[2];
		outputPath=args[3];
		threshold = new Double(args[4]);
		
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Ex44");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the first input file (WHATCED FILM)
		JavaRDD<String> visualizationRDD = sc.textFile(inputPath);
		
		// Read the content of the second input file (PREFERENCE)
		JavaRDD<String> preferenceRDD = sc.textFile(inputPath2);
		
		// Read the content of the third input file (MOVIES INFO)
		JavaRDD<String> infoMovieRDD = sc.textFile(inputPath3);
		
		//map to pair (userId,movie-genre) of liked movies
		JavaPairRDD<String,String> userPreferenceGenreRDD = preferenceRDD.mapToPair((record)->{
			String[] fields= record.split(",");
			Tuple2<String,String> pair = new Tuple2<String,String>(fields[0],fields[1]);
			return pair;
		});
			
		//map to pair (genreId,userId) of whatced movies
		JavaPairRDD<String,String> userWhatchedGenreIdRDD = visualizationRDD.mapToPair((record)->{
			String[] fields= record.split(",");
			Tuple2<String,String> pair = new Tuple2<String,String>(fields[1],fields[0]);
			return pair;
		});
		
		//map to pair (genreId,genre) of whatced movies
		JavaPairRDD<String,String> genreIDgenreRDD = infoMovieRDD.mapToPair((record)->{
			String[] fields= record.split(",");
			Tuple2<String,String> pair = new Tuple2<String,String>(fields[0],fields[2]);
			return pair;
		});
		
		JavaPairRDD<String,Tuple2<String,String>> mappingUserGenreRDD = userWhatchedGenreIdRDD.join(genreIDgenreRDD);
		
		//now have genreid , userid , genre
		//select only userid , genre with map to pair (userId,genre)
		JavaPairRDD<String,String> userWhatcedGenreRDD = mappingUserGenreRDD.mapToPair((Tuple2<String,Tuple2<String,String>>userMovie)->{
			String user;
			String genre;
			user = userMovie._2()._1();
			genre = userMovie._2()._2();
			Tuple2<String,String> pair = new Tuple2<String,String> (user,genre);
			return pair;
		});
		
		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> listUserGenreLikedAndViasualizedRDD = userWhatcedGenreRDD.cogroup(userPreferenceGenreRDD);
		
		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> filteredRDD = listUserGenreLikedAndViasualizedRDD.filter(
				(Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>line)->{
				int notLiked = 0;
				int total = 0;
				
				ArrayList<String> likedGenres = new ArrayList<String>();

				for (String likedGenre : line._2()._2()) {
					likedGenres.add(likedGenre);
				}
				
				for (String whatched : line._2()._1()) {
					boolean find = false;
					for (String liked : likedGenres) {
						if (whatched.contains(liked)) {
							find = true;
						}
					}
					if (find == true) {
						total++;
					}else {
						notLiked++;
						total++;
					}
					
				}
				Double percentage = new Double((double)notLiked/(double)total*100);
				if (percentage > threshold) {
					return true;
				}else {
					return false;
				}
					
	
		});
		
		// Select only the userid of the users with a misleading profile
		JavaRDD<String> misleadingUsersRDD = filteredRDD.keys();
				
		
		misleadingUsersRDD.saveAsTextFile(outputPath);


		// Close the Spark context
		sc.close();
	}
}

