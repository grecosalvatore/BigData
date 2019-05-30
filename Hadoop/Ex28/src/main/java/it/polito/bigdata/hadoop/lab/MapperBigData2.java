package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MAPPER - MANAGE ANSWERS
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	String[] fields = value.toString().split(",");
		String questionId = fields[1];
		String answerId = fields[0];
		String textAnswer = answerId + ":\t";
		textAnswer = textAnswer + fields[3]; 
		
			context.write(new Text(questionId), new Text(textAnswer));
    		
           
           
          
    }
}
