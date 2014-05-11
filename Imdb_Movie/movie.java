package movie;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class movie
{
	//Mapper class extracts the data from ratings.dat file and takes userID from it, adds a number 1 as value and sends to reducer.
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		
		    private Text userID = new Text();
		   
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		    {
		        String line = value.toString();     
				String[] d = line.split("::");
		        String users = d[0];
	        	userID.set(users);
		        context.write(userID, new IntWritable(1));
		    } 
	
	}
	
	
	//Reducer takes all userID inputs as keys, maintains a count for each user, if count is greater than 40 writes
	//the User data to HDFS along with the corresponding user count.
	public static class Reduce extends Reducer<Text, IntWritable, Text,IntWritable>
	{
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws 
		 IOException, InterruptedException 
		 {
			 int count = 0;
			 for (IntWritable val : values) 
				 count += val.get();
	       
			 if (count>40)
				 context.write(key, new IntWritable(count));
		 } 
		 
	}
	
	
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "movie");
		
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class); 
		job.setJarByClass(movie.class);
		
		job.setMapperClass(Map.class); 
		//job.setCombinerClass(Reduce.class); 
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true); 
		} 
		
	}
