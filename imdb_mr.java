import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class imdb_mr {
// Mappers
static class TitleActorsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // FileSplit fileSplit = (FileSplit) context.getInputSplit();
        // String filename = fileSplit.getPath().getName();
        String str = value.toString();
        String[] result = str.split(",");
        // skipping header
        if (key.get()>0) {
            String tconst = result[0];
            String actorid = result[1];
            String actorName = result[2];

            // Emit key-value pair with movie ID as key and actor name as value
            context.write(new Text(tconst), new Text("Actor\t"+actorid+"\t"+actorName));
        }
    }
}


static class TitleBasicsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // FileSplit fileSplit = (FileSplit) context.getInputSplit();
        // String filename = fileSplit.getPath().getName();
        String str = value.toString();
        String[] result = str.split("\t");

        // Assuming you have at least 9 columns in each row
        if (key.get()>0) {
            String tconst = result[0];
            String titleType = result[1];
            String primaryTitle = result[2];
            String startYear = result[5];
            String genre = result[8];

            if(!startYear.equals("\\N")  && Integer.parseInt(startYear) >= 1960 && Integer.parseInt(startYear) <= 1970 && titleType.equals("tvMovie")){
                // Emit key-value pair with movie ID as key and movie title as value
                context.write(new Text(tconst), new Text("Title\t" + primaryTitle + "\t" + genre + "\t" + startYear));
            }
        }
    }
}



static class TitleCrewMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // FileSplit fileSplit = (FileSplit) context.getInputSplit();
        // String filename = fileSplit.getPath().getName();
        String str = value.toString();
        String[] result = str.split("\t");

        String tconst = result[0];
        String directors = result[1];

        if(!directors.equals("\\N") && key.get()>0)
          // Emit key-value pair with movie ID as key and directors as value
          context.write(new Text(tconst), new Text("Crew\t"+directors));
    }
}


// Reducer
static class MovieActorDirectorReducer extends Reducer<Text, Text, Text, Text> {
  // Implementation of reduce method
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    String title = "";
    String director = "";
    String actor = "";
    String genre = "";
    String year = "";
    String actorName = "";

    for (Text value : values) {
        String[] result = value.toString().split("\t");

        if (result[0].equals("Title")) {
            // Data from TitleBasicsMapper
            title = result[1];
            genre = result[2];
            year = result[3];
        } else if (result[0].equals("Crew")) {
            // Data from TitleCrewMapper
            director = result[1];
        } else if (result[0].equals("Actor")) {
            // Data from TitleActorsMapper
            actor = result[1];
            actorName = result[2];
        }
    }

    String[] directors = director.split(",");
    boolean actorIsDirector = false;
    // Iterate through the array of directors
    for (String directorName : directors) {
        // Trim the director's name to remove leading/trailing spaces
        directorName = directorName.trim();

        // Check if the actor's name matches the director's name
        if (actor.equals(directorName))
          actorIsDirector = true;
    }

    if(!actor.isEmpty() && (actor.equals(director) || actorIsDirector) && !title.isEmpty())
      context.write(new Text(key), new Text(title + ", " + actorName + ", " + actor + ", " + director + ", " + genre + ", " + year));
    // }
  }
}

  // Driver code
  // Configuration and job setup
  public static void main(String[] args) throws Exception {
      // Job setup, configuration, and input paths
      Configuration conf = new Configuration();
      int split = 730*1024*1024; // This is in bytes
      String splitsize = Integer.toString(split);
      conf.set("mapreduce.input.fileinputformat.split.minsize",splitsize);
      Job job1 = Job.getInstance(conf, "actor-director gig");
      // job1.setNumReduceTasks(2);
      job1.setJarByClass(imdb_mr.class);
      MultipleInputs.addInputPath(job1,new Path(args[0]), TextInputFormat
      .class, TitleBasicsMapper.class);
      MultipleInputs.addInputPath(job1,new Path(args[1]), TextInputFormat
      .class, TitleActorsMapper.class);
      MultipleInputs.addInputPath(job1,new Path(args[2]), TextInputFormat
      .class, TitleCrewMapper.class);
      job1.setReducerClass(MovieActorDirectorReducer.class);
      job1.setOutputValueClass(Text.class);
      job1.setOutputKeyClass(Text.class);
      FileOutputFormat.setOutputPath(job1, new Path(args[3]));
      job1.waitForCompletion(true);
      System.exit(0);
  }
}
