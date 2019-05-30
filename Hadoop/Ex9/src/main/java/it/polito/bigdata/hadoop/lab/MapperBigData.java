package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.security.KeyStore.Entry;
import java.util.HashMap;

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
                    IntWritable> {// Output value type
	
    //declare the hasmap that will contain the words in main memory
	HashMap<String, Integer> wordsCounts;
	
	protected void setup(Context context) {
		//create one time the hashamp
		wordsCounts = new HashMap<String, Integer>();
	}
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// Split each sentence in words. Use whitespace(s) as delimiter (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
    		String[] words = value.toString().split(" ");
    		
    		Integer tmpFreq;
    		
    		// Iterate over the set of words
    		for(String word : words) {
    			
    			// Transform word case
    			String cleanedWord = word.toLowerCase();
    			
    			//check if the word is already in the hashmap
    			tmpFreq = wordsCounts.get(cleanedWord);
    			if (tmpFreq != null) {
    				//the word is already in the hashmap then update the value
    				tmpFreq++;
    				wordsCounts.put(cleanedWord, tmpFreq);
    			}else {
    				wordsCounts.put(cleanedWord, 1);
    			}
             }  
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {

		// Emit the set of (key, value) pairs of this mapper
		for (java.util.Map.Entry<String, Integer> pair : wordsCounts.entrySet()) {
			context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
		}

	}
}
