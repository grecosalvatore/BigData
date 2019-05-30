package it.polito.bigdata.hadoop.lab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
	
	ArrayList<String> rules;
	
	protected void setup(Context context) throws IOException, InterruptedException{
		String line;
		
		//retrive the dictionary
		Path[] PathsCachedFiles = context.getLocalCacheFiles();
		
		BufferedReader file = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));
		
		//create the arraylist
		rules = new ArrayList();
		
		//read content of the file
		while ((line = file.readLine()) != null) {
			rules.add(line);
		}
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// Split each sentence in words. Use whitespace(s) as delimiter (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
    		String outRecord = value.toString();
    		String[] fields = value.toString().split(",");
    		
    		String gender = fields[3];
    		String yearOfBirth = fields [4];
    		
    		String currentRule = "Gender=" + gender + " and " + "YearOfBirth=" + yearOfBirth;
    		
    		Boolean find = false;
    		String outCategory="";
    		for (String rule : rules) {
    			String[] ruleFields = rule.split(" -> ");
    			String body = ruleFields[0];
    			String rightSide = ruleFields[1];
    			
    			if (currentRule.equals(body)) {
    				outCategory = rightSide;
    				find = true;
    				break;
    			}
    		}
    		
    		if (find == false) {
    			outCategory = "Unknown";
    		}
    		
    		outRecord = outRecord + "," + outCategory;
    		
   			context.write(NullWritable.get(), new Text(outRecord));
   			
    	           
    		
           
           
          
    }
}
