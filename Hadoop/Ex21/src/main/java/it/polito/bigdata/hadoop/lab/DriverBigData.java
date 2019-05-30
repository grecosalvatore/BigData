package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

	public static enum COUNTERS{
		id;
	}
	
  @Override
  public int run(String[] args) throws Exception {

	
	  
	int exitCode;  
	
    Configuration conf = this.getConf();

    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Ex21");
 


    Path inputPath1,inputPath2;
    Path outputDir;


	
	// Parse the parameters
    inputPath1 = new Path(args[0]);
    inputPath2 = new Path(args[1]);
    outputDir = new Path(args[2]);
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath1);
    
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    //set shared/cached file
    job.addCacheFile(inputPath2.toUri());
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    // Set job input format
    job.setInputFormatClass(TextInputFormat.class);

    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
       
    // Set map class
    job.setMapperClass(MapperBigData.class);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    

    // Set number of reducers
    job.setNumReduceTasks(0);
    

     
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true)
    	exitCode=0;
    else
    	exitCode=1;
    	
    return exitCode;
    
    
  }
  

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

    System.exit(res);
  }
}
