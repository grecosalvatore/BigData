package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.polito.bigdata.hadoop.lab.DriverBigData.COUNTERS;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	String userName = key.toString();
    	String listFriends = "";
    	
    	for (Text value : values) {
    		if (listFriends.length() == 0) {
    			listFriends = value.toString();
    		}else {
    			listFriends = listFriends + " " + value.toString(); 
    		}
    	}
    	if (listFriends.length() != 0) {
    		context.write(new Text(userName), new Text(listFriends));
    	}
    }
}
