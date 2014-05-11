package MovieJoin1;


import java.io.*;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MovieJoin1 {


    public static class MapJoin1 extends Mapper<LongWritable, Text, Text, IntWritable> 
    {
    	//Mapper takes ratings.dat file input, extracts userID and add IntWritable 1 to it and sends it to reducer.
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
    
	//Reducer counts the userID and finds the top 10 users and write to HDFS.
    public static class ReduceJoin1 extends Reducer<Text, IntWritable, NullWritable, Text>
    {
	
    	private PriorityQueue<compare_count> queue = new PriorityQueue<compare_count>();
		
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws 
    	IOException, InterruptedException 
    	{
    		int count = 0;
    		for (IntWritable val : values) 
    			count += val.get();
		  
    		String userid = key.toString(); 
		 	
    		compare_count count_object = new compare_count(userid,count);
    		queue.add(count_object);
    	}
		              
    	@Override
    	protected void cleanup(Context context) throws 
    	IOException, InterruptedException 
    	{
		    	
    		String[] userid = new String [10];
    		String[] count = new String [10];
	        
    		int c = 0;
    		while (c < 10 && !queue.isEmpty())
    		{    	
			//Stores userID and count in array pulling it out from the queue.
			
			compare_count count_object = queue.poll();
			userid[c] = count_object.userid1;
			count[c] = Integer.toString(count_object.count1);
			c++;  
    		}
			        
    		//Writes the String in inverse to HDFS to get the descending order.
    		//Not needed  to print in descending order here but for our verification.
    		for(int i = 1; i<=10; i++)
			context.write(NullWritable.get(),new Text(userid[i-1]+"::"+count[i-1]));
			        	
    	} 
	 
    } 
	 

  //Finds the top 10 users and returns that.
    
	static class compare_count implements Comparable<compare_count> 
	{ 
		public String userid1 = null;
		public int count1 = 0;
		public compare_count(String userid1, int count1) 
		{ 	  
			this.userid1 = userid1; 
			this.count1 = count1; 
		} 
	  
		@Override 
		public int compareTo(compare_count other)
		{
			if(count1 < other.count1)
				return 1;
		  
			else
				return -1;
	
		}	
	}

	
	//MapJoin2 reads one file from DistributedCache i.e. the output of reducer 1 and the other input file i.e.
	//users.dat and joins the two based on userID.
	
    public static class MapJoin2 extends Mapper<LongWritable, Text, Text, Text> 
    {

    	private static Map<String, String> map = new HashMap<String, String>();;
        
        
        private Text UserInfo=new Text();
        
        File f;
        @Override
        public void setup(Context context) 
        {
        	 try{	 
        		 	Path[] P = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        		 	f = new File(P[0].toString());
        		 	BufferedReader br = new BufferedReader(new FileReader(f));
        		 	String line="";
        		 	try
        		 	{
        		 		while((line = br.readLine()) != null)
        		 		{
        		 			String[] d = line.split("::");
	    			
        		 			map.put(d[0], d[1]);
	    			
        		 		}
	    	
        		 		System.out.println(map);
        		 	}
        		 	finally 
        		 	{
        		 		br.close();
        		 	}	
	    		 
        	 	}
        	 	catch(Exception e)
        	 	{
        	 		e.printStackTrace();
        	 	}
        }
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
        			
        	String line = value.toString();
        	String[] d = line.split("::");
        	String Userid = d[0];
        	
        	if( map.get(d[0]) != null)
        	{
        		UserInfo.set(new Text(Userid+"\t"+d[1]+"\t"+d[2].replaceAll("::", "\t")+"\t"+(map.get(d[0]))));
        		context.write(new Text(map.get(d[0])) , UserInfo);
        	}
        }
    
    }

    
    //ReduceJoin2 prints the UserID, sex, age and count in descending order of count.
    
    public static class ReduceJoin2 extends Reducer<Text, Text, Text, Text> 
    {
       
        	public void reduce(Text key, Iterable<Text> value,Context context) 
        			throws IOException, InterruptedException 
        	{
            	for(Text val : value)	
            		context.write(null,  val);
        	} 
    } 
   
    //DescendingKeyComparator overrides the Sorting method of hadoop and prints in descending order.
    public static class DescendingKeyComparator extends WritableComparator 
    {
        protected DescendingKeyComparator() 
        {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) 
        {
            Text k1 = (Text) w1;
            Text k2 = (Text) w2;

        return -1 * k1.compareTo(k2);
        }
    } 

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MovieJoin1");
        Path ratingsFile = new Path(args[0] + "ratings.dat");
        Path userFile = new Path(args[0] + "users.dat");
        Path Map1Output = new Path(args[1]);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJarByClass(MovieJoin1.class);

        job.setMapperClass(MapJoin1.class);
        job.setReducerClass(ReduceJoin1.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, ratingsFile);
        FileOutputFormat.setOutputPath(job, Map1Output);

        if (job.waitForCompletion(true)) 
        {
            Configuration conf2 = new Configuration();
          
            Path Intermediate = new Path(Map1Output+"/part-r-00000");
            DistributedCache.addCacheFile(Intermediate.toUri(), conf2);
            Job job2 = new Job(conf2, "MovieJoin1");
            
            job2.setJarByClass(MovieJoin1.class);
            job2.setMapperClass(MapJoin2.class);
            job2.setReducerClass(ReduceJoin2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setSortComparatorClass(DescendingKeyComparator.class);

            FileInputFormat.addInputPath(job2,userFile );
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            job2.waitForCompletion(true);

        }
    }

}