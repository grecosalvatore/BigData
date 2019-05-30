package it.polito.bigdata.hadoop.lab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

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
                    NullWritable,         // Output key type
                    Text> {// Output value type
	
	HashMap<String,Integer> mapping;
	
	protected void setup(Context context) throws IOException, InterruptedException{
		String line;
		
		//retrive the dictionary
		Path[] PathsCachedFiles = context.getLocalCacheFiles();
		
		BufferedReader file = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));
		
		//create the hashmap
		mapping = new HashMap();
		
		//read content of the file
		while ((line = file.readLine()) != null) {
			String word;
			Integer id;
			
			// line = "word\tnumber\n"
			String[] fields = line.split("\t");
			word = fields[1];
			id = new Integer(fields[0]);
			if ((mapping.get(word)) == null ) {
				mapping.put(word, id);
			}
		}
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// Split each sentence in words. Use whitespace(s) as delimiter (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
    		String[] words = value.toString().split(" ");
    		String outputLine = "";
    		Integer tmpId;
    		for (String word : words) {
    			String cleanedWord = word.toUpperCase();
    			tmpId = mapping.get(cleanedWord);
    			if (tmpId != null) {
    				if (outputLine.length() == 0) {
    					outputLine = tmpId.toString();
    				}else {
    					outputLine = outputLine + " " + tmpId.toString();
    				}
    			}
    		}
    		
   			context.write(NullWritable.get(), new Text(outputLine));
   			
    	           
    		
           
           
          
    }
}
