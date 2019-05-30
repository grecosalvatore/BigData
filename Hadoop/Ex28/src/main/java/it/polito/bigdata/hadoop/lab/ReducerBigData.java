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
    	
    	String question="";
    	ArrayList<String>answers = new ArrayList<String>();
    	
    	for (Text value : values) {
    		String[] fields = value.toString().split(":\t");
    		if (fields[0].equals("Q")){
    			//if there is the question
    			question = fields[1];
    		}else {
    			//if there is an answer
    			answers.add(value.toString());
    		}
    		
    	}
    	String outText ="";
		outText = key.toString() + "," + question + ",";
    	for (String answer : answers) {
    		String[] fields = answer.split(":\t");
    		outText = outText + fields[0] + "," + fields[1];
    		context.write(NullWritable.get(), new Text(outText));
    	}
    	
    }
    	
}
