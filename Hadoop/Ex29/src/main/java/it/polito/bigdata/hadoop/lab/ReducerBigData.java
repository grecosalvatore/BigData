package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	Boolean findCommedia = false;
    	Boolean findAdventure = false;
    	String outRecord = "";
    	
    	for (Text value : values) {
    		String[] fields = value.toString().split(":\t");
    		if (fields[0].equals("U")) {
    			//this record come from user file
    			outRecord = fields[1];
    		}
    		if (fields[0].equals("G")) {
    			if (fields[1].toLowerCase().equals("commedia")) {
    				findCommedia = true;
    			}
    			if (fields[1].toLowerCase().equals("adventure")) {
    				findAdventure = true;
    			}
    		}	
    	}//end for
    	
    	if ( (findCommedia==true) & (findAdventure==true) ) {
    		context.write(NullWritable.get(), new Text(outRecord));
    	}

    		
    	
    	
    }
    	
}
