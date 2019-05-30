package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	float min = Float.MAX_VALUE;
    	float max = -Float.MAX_VALUE;
    	
    	 for(FloatWritable value : values) {
         	if (value.get() > max) {
         		max=value.get(); 
         	}
         	if (value.get() < min) {
         		min=value.get();
         	}
         }
    	 String tmpOut = "max=" + Float.toString(max) + "min=" + Float.toString(min);
    	 context.write(new Text(key), new Text(tmpOut));
    	 
    }
}
