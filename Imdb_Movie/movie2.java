package movie2;


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



public class movie2
{

	//Mapper takes users.dat file input, extracts zipcode and user's age from it and sends it to reducer.
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		  private IntWritable age = new IntWritable();
		  private Text zipcode = new Text();
		   
		  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		  {
		        String line = value.toString();     
				String[] d = line.split("::");
		        Integer age1 = Integer.parseInt(d[2]);
		        String zipcode1 = d[4];
	        	zipcode.set(zipcode1);
	        	age.set(age1);
		        context.write(zipcode, age);
		  } 
	
	}
	
	//Reducer finds average of user ages having the same zipcode. Zipcode is the key. It writes the zipcode and its
	//corresponding average age to the HDFS.
	
	public static class Reduce extends Reducer<Text, IntWritable, Text,Text>
	{
		private Text avg_age = new Text();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws 
		IOException, InterruptedException 
		{
			 int sum_age = 0, count = 0;
			 for (IntWritable val : values) 
			 {
				 //Summing up all the ages.
				 sum_age += val.get();
				 count++;
			 }
		 
		 	float avg_age1 = (float) sum_age/count;
		 	avg_age.set(":"+String.valueOf(avg_age1));
			context.write(key,(avg_age));
			
		 } 
	}
	
	//Mapper2 takes reducer's output from the HDFS and calls the comparable class to find the youngest users and
	//then stores the youngest average age in string array and also stores the zipcode in a array.
	
	 public static class Map2 extends Mapper<Object, Text, NullWritable, Text>
	 {
	    private PriorityQueue<compare_age> queue = new PriorityQueue<compare_age>();
		
		
	    public void map(Object key, Text value, Context context) throws 
	    IOException, InterruptedException
	    {
	    	
	    	String[] val = value.toString().split(":");
	    
	    	String zipcode1 = val[0]; 
	    	String avg_age1 = val[1];
	    	
	    	compare_age age_object = new compare_age(zipcode1,Double.parseDouble(avg_age1)) ;
	    	queue.add(age_object);
		}
	              
	    @Override
	    protected void cleanup(Context context) throws 
	    IOException, InterruptedException 
	    {
	    	
	    	 String[] zipcode = new String [10];
		     String[] avg_age = new String [10];
		        
		     int c = 0;
		     while (c < 10 && !queue.isEmpty())
		     {    	
		    	 
		     //Stores zipcode and average age in array pulling it out from the queue.
		    	 
		     compare_age age_object = queue.poll();
		     zipcode[c] = age_object.zipcode1;
		     avg_age[c] = Double.toString(age_object.avg_age1);
		     c++;  
		     }
		        
		     //Writes the String in inverse to HDFS to get the descending order.
		     for(int i = 10; i>0; i--)
		        context.write(NullWritable.get(),new Text(zipcode[i-1]+":"+avg_age[i-1]));
		        	
	    } 
	 }

	    
	 //Finds the youngest average age and returns that.
	static class compare_age implements Comparable<compare_age> 
	{ 
		private String zipcode1 = null;
		private double avg_age1 = 0.0;
		public compare_age(String zipcode1, double avg_age1) 
		{ 	  
			this.zipcode1 = zipcode1; 
			this.avg_age1 = avg_age1; 
		} 
	  
		@Override 
		public int compareTo(compare_age other)
		{
			if(avg_age1 > other.avg_age1)
				return 1;
		  
			else
				return -1;
	
		}	
	}


	
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "movie2");
		
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class); 
		job.setJarByClass(movie2.class);
		
		job.setMapperClass(Map.class); 
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		if(job.waitForCompletion(true));
		{
            Job YoungestCountingJob = new Job(conf, "movie2_young_age");
            YoungestCountingJob.setJarByClass(movie2.class);
            YoungestCountingJob.setMapperClass(Map2.class);

            YoungestCountingJob.setOutputKeyClass(NullWritable.class);
            YoungestCountingJob.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(YoungestCountingJob, new Path(args[1]));
            FileOutputFormat.setOutputPath(YoungestCountingJob, new Path(args[2]));
            
            System.exit(YoungestCountingJob.waitForCompletion(true) ? 0 : 1);
		} 
	} 
		
}
