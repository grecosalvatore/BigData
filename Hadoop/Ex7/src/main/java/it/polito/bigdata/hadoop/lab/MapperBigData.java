package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// Split each sentence in words. Use whitespace(s) as delimiter (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
    		String[] fields = value.toString().split("\t");
    		String sentenceId = fields[0];
    		String[] words = fields[1].split(" ");
    		
    		// Iterate over the set of words
    		for(String word : words) {
             	// Transform word case
                 String cleanedWord = word.toLowerCase();
                 if (cleanedWord.equals("and")||cleanedWord.equals("or")||cleanedWord.equals("not")) {
                	 //don t do nothing
                 }else {
                 // emit the pair (word, sentenceID)
                 context.write(new Text(cleanedWord), new Text(sentenceId));
                 }
             }
            
          
    }
}
