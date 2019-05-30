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
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    DateIncome> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// Split each sentence in words. Use whitespace(s) as delimiter (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
    		String[] fields = value.toString().split("\t");
    		String date = fields[0];
    		int income = Integer.parseInt(fields[1].toString());
    		DateIncome tmpDateIncome = new DateIncome();
    		tmpDateIncome.setDate(date);
    		tmpDateIncome.setIncome(income);
    		// Iterate over the set of words
           
            // emit the pair (word, 1)
            context.write(NullWritable.get(), tmpDateIncome);
          
    }
}
