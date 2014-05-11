package MovieJoin2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovieJoin2 {
    
    //MapRatings extracts data from ratings.dat file and gets userID, MovieID and Rating from it.
    //We add a char 'R' to extract and store in list later in reducer.
     public static class MapRatings extends Mapper<LongWritable, Text, Text, Text> 
     {

            private Text userID = new Text();
            private Text Rating = new Text();

            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
            {
                String line = value.toString().trim();
                String[] d = line.split("::"); 
                try 
                {
                    userID.set(d[0]);
                    Rating.set("R" +d[1]+";"+d[2]);
                    context.write(userID, Rating); 
                    
                } catch (ArrayIndexOutOfBoundsException e) {}
            }
        }


    //MapUser extracts data from users.dat file and gets userID corresponding to male 'M' users from it.
    // We add a char 'U' to extract and store in list later in reducer.
     
    public static class MapUser extends Mapper<LongWritable, Text, Text, Text> 
    {

        private Text userID = new Text();
        private Text userGender = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            String[] d = line.split("::");
            try 
            {    
                if (d[1].equals("M")) 
                {
                    userID.set(d[0]);
                    userGender.set("U" + d[1]);
                    context.write(userID, userGender); 
                }
            } catch (ArrayIndexOutOfBoundsException e) {}
        }
    }

   //Reducer joins the user and rating information recieved from mappers based on userID.
    public static class ReduceJoin extends Reducer<Text, Text, Text, Text> 
    {
     
        private Text  userID = new Text();
        private Text temp = new Text();
        private Text movieID = new Text();
        
        private ArrayList<Text> UserList = new ArrayList<Text>();
        private ArrayList<Text> RatingList = new ArrayList<Text>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            UserList.clear();
            RatingList.clear();
            
            String[] rating = null;
            Iterator<Text> valueIterator = values.iterator();
            
            while (valueIterator.hasNext())
            {
                //Store data to ArrayLists.
                temp = valueIterator.next();
                if (temp.charAt(0) == 'U')
                    UserList.add(new Text(temp.toString().substring(1)));
                
                else if (temp.charAt(0) == 'R')
                    RatingList.add(new Text(temp.toString().substring(1)));
                
            }
            
            //Join logic.
            if (!UserList.isEmpty() && !RatingList.isEmpty()) 
            {
                for (Text users : UserList) 
                {
                    for (Text ratings : RatingList) 
                    {
                        try
                        {
                            rating = ratings.toString().split(";");
                            userID.set(rating[1]);
                            movieID.set(rating[0]);
                            context.write(movieID, userID);
                            
                        } catch (Exception e) {}
                    }
                }
            }
        }
    }

    
    //MapUserRatings extracts data from output file of ReducerJoin.
    // We add a char 'T' to extract and store in list later in reducer.

    public static class MapUserRatings extends Mapper<LongWritable, Text, Text, Text> 
    {

        private Text userID = new Text();
        private Text movieID = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            String[] d = line.split("[\\s]+");
            try 
            {
                userID.set(d[0]);
                movieID.set("T"+d[1]);
                context.write(userID, movieID);
                
            } catch (Exception e) {}
        }
    }

  //MapMovies extracts data from movies.dat file and gets movie information from it.
  // We add a char 'M' to extract and store in list later in reducer. 
    
    public static class MapMovies extends Mapper<LongWritable, Text, Text, Text> 
    {

        private final static Text movieID = new Text();
        private static Text Genre = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            String[] d = line.split("::");
            String[] genres = d[2].trim().split("\\|");
            
            String choice = "Action OR Drama";
            
            for (String genre : genres) 
            {
                if(choice.contains(genre))
                {
                    movieID.set(d[0]);
                    Genre.set("M"+d[1]+"\t"+d[2]);
                    context.write(movieID, Genre);
                    break;
                }
            }
        }
    }

    //Reducer joins the movie and MapUserRating information recieved from mappers based on movieID.
    public static class FinalReduceJoin extends Reducer<Text, Text, Text, Text> 
    {

        private Text MovieInfo = new Text();
        private Text temp = new Text();
        private Text MovieID = new Text();
        
        private ArrayList<Text> MovieList = new ArrayList<Text>();
        private ArrayList<Text> RatingList = new ArrayList<Text>();
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            MovieList.clear();
            RatingList.clear();
            int sum = 0, count = 0;
            
            String movieID = key.toString();
            Iterator<Text> valueIterator = values.iterator();
            
            while (valueIterator.hasNext()) 
            {
                temp = valueIterator.next();
                if (temp.charAt(0) == 'M')
                    MovieList.add(new Text(temp.toString().substring(1)));
                
                else if (temp.charAt(0) == 'T')
                {
                    int rating = Integer.parseInt(temp.toString().substring(1));
                    sum += rating;
                    count++;
                }
            }
            
            //Find the average rating for that movie
            
            double avg = (double) sum / count;
                                    
            //If rating is between 4.4 and 4.7, write to HDFS.
            if (!MovieList.isEmpty() && avg >= 4.4 && avg<= 4.7) 
            {
                MovieInfo.set(MovieList.get(0)+"\t"+avg);
                MovieID.set(movieID);
                 context.write(MovieID, MovieInfo);
            }
        }
    }

    public static void main(String[] args) throws Exception 
    {

        JobConf conf = new JobConf();
        Job JoinJob1 = new Job(conf, "MovieJoin2");

        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(JoinJob1, new Path(args[0]), TextInputFormat.class, MapRatings.class);
        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(JoinJob1, new Path(args[1]), TextInputFormat.class, MapUser.class);
        
       


        JoinJob1.setReducerClass(ReduceJoin.class);
        JoinJob1.setMapOutputKeyClass(Text.class);
        JoinJob1.setMapOutputValueClass(Text.class);
        JoinJob1.setOutputKeyClass(Text.class);
        JoinJob1.setOutputValueClass(Text.class);
        JoinJob1.setJarByClass(MovieJoin2.class);

        JoinJob1.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(JoinJob1, new Path(args[2]));

        if (JoinJob1.waitForCompletion(true)) {

            JobConf conf2 = new JobConf();
            Job JoinJob2 = new Job(conf2, "MovieJoin2");

            org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(JoinJob2, new Path(args[2]), TextInputFormat.class, MapUserRatings.class);
            org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(JoinJob2, new Path(args[3]), TextInputFormat.class, MapMovies.class);
            

            JoinJob2.setReducerClass(FinalReduceJoin.class);
            JoinJob2.setMapOutputKeyClass(Text.class);
            JoinJob2.setMapOutputValueClass(Text.class);
            JoinJob2.setOutputKeyClass(Text.class);
            JoinJob2.setOutputValueClass(Text.class);
            JoinJob2.setJarByClass(MovieJoin2.class);

            JoinJob2.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(JoinJob2, new Path(args[4]));

            JoinJob2.waitForCompletion(true);

        }
    }
}
