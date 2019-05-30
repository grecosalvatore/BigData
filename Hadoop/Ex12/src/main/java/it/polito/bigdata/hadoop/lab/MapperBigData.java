package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
                    Text,         // Output key type
                    NullWritable> {// Output value type
	double threshold;
	
	protected void setup(Context context) {
		// I retrieve the value of the threshold only one time for each mapper
		threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		String line;
    		
    		line = value.toString();
    	
    		String[] fields = line.split("\t");
    		double pm10 = Double.parseDouble(fields[1]);
           
    		if(pm10 < threshold) {                
                // emit the entirely record
                context.write(new Text(line),null);
            }
    }
}
