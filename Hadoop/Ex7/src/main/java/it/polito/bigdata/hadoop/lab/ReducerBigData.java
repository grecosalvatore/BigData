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
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	String tmpOut = "";
    	 for(Text value : values) {
    		 if (tmpOut.length() == 0){
    			 tmpOut = value.toString();
    		 }else {
    			 tmpOut = tmpOut + "," + value.toString();
    		 }
    	}
    	
    	 context.write(new Text(key), new Text(tmpOut));
    	 
    }
}
