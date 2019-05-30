package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// Split each sentence in words. Use whitespace(s) as delimiter (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
    		String[] fields = value.toString().split(",");
    		String zoneId = fields[0];
    		String[] tmpFields = fields[1].split("\t");
    		String date = tmpFields[0];
    		Double pm10 = new Double(tmpFields[1].toString());
    		
    		// Iterate over the set of words
            if (pm10 > 50) {
                
                // emit the pair (word, 1)
                context.write(new Text(zoneId), new Text(date));
            }
    }
}
