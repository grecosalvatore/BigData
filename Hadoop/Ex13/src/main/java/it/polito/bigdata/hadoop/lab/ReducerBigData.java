package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                DateIncome,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<DateIncome> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	int max=0;
    	String tmpOut = "";
    	
    	 for(DateIncome value : values) {
         	if (value.getIncome() > max) {
         		max = value.getIncome();
         		tmpOut = value.getDate() + "\t" + value.getIncome();
         	}
         }
    	
    	 context.write(null, new Text(tmpOut));
    	 
    }
}
