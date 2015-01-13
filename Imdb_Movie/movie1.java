/***
 * @author 
 * Anchal Katyal
 * 
 * 
 * Decsription:
 * The code reads data from IMDB file and finds the genres of the movies.
 * 
 * 
 */




package movie1;


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



public class movie1
{

	//Mapper takes movie data from movies.dat file matched the movie names recieved from command line and then
	//send genres as the key with a value of one.
	
	public static class Map extends Mapper<LongWritable, Text, Text , IntWritable> 
	{
		String ip_parameter = "";   
		String[] ip_array = null;
		 
		
		//Gets input parameter entered at command line. (It splits movie names by ';').
		protected void setup(Context context) throws IOException, InterruptedException
		{ 

			Configuration conf = context.getConfiguration(); 
			
			ip_parameter = conf.get("parameter");   
		    ip_array = ip_parameter.split(";");    
		    
		}
	
		private Text genre = new Text();

			   
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    {			        
	    	String line = value.toString();     
	    	String[] d = line.split("::");
	    	
	    	//Extract movie data from movie.dat file and stores movie names in a string array.
	    	String movie_n = d[1];
	
	    	//Extracts genres and store in string array.
	    	
	        String m_genre = d[2].toString();
	        
	        //split_genre splits genres related to a single movie.
		    String[] split_genre = m_genre.split("\\|");
	      

		        for(int i=0;i<ip_array.length;i++)
		        {
		        	
		        		if(ip_array[i].equalsIgnoreCase(movie_n))
		        		{
		        			
		        			//As long as genres exists for that movie send to reducer.
		        			for (int j=0; j<split_genre.length; j++)
		        	        {
		        				genre.set(split_genre[j]);
		        				context.write(genre,new IntWritable(1));	
		        	        }
		        		}
		        		
		        }     
		} 
		
	}
	
	
	//Reducer takes genres as key and prints the key once on HDFS with a null value.
	
		public static class Reduce extends Reducer<Text, IntWritable, Text,Text>
		{
			
			 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws 
			 IOException, InterruptedException 
			 {
				context.write(key,null);
			 } 
			 
		}

		
		public static void main(String args[]) throws Exception
		{
			Configuration conf = new Configuration();
			
			conf.set("parameter", args[2]);
			
		//	conf.set("mapreduce.output.key.field.separator", ",");
			
			Job job = new Job(conf, "movie1");
			
			job.setOutputKeyClass(Text.class); 
			job.setOutputValueClass(IntWritable.class); 
			job.setJarByClass(movie1.class);
			
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