package it.polito.bigdata.hadoop.lab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
    
	private ArrayList<String> stopWords;
	
	
	protected void setup(Context context) throws IOException, InterruptedException {
		
		String line;
		
		//retrive the paths of the local copes of the distributed cache
		Path[] PathsCachedFiles = context.getLocalCacheFiles();
		
		//read the content of the cached file and process it
		BufferedReader file = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));
		
		stopWords=new ArrayList<String>();
		
		//read lines
		while ((line = file.readLine())!=null) {
			//one stop word per line
			stopWords.add(line);
		}
		
		file.close();
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// Split each sentence in words. Use whitespace(s) as delimiter (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
    		String[] words = value.toString().split(" ");
    		
    		String sentenceNoStopWords = "" ;
    		
    		// Iterate over the set of words
           for (String word : words) {
        	   
        	   if ((stopWords.contains(word)) == false) {
        		   //this word is not a stopword
        		   sentenceNoStopWords = sentenceNoStopWords + " " + word + " ";
        	   }
        	  
           }
           
           if ((sentenceNoStopWords.equals("")) == false) {
        	   // emit the pair (word, null) for each word
        	   context.write(new Text(sentenceNoStopWords), NullWritable.get());
           }
          
    }
}
