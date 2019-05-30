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
                NullWritable,    // Input value type
                Text,           // Output key type
                LongWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<NullWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	String word = key.toString();
    	
    	context.getCounter(COUNTERS.id).increment(1);;
    	long id = context.getCounter(COUNTERS.id).getValue();
    	 context.write(new Text(word), new LongWritable(id));
    	 
    }
}
