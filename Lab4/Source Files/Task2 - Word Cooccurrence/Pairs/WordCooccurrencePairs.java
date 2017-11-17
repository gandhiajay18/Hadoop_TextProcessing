import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCooccurrencePairs{

  public static class TokenizerMapper extends Mapper<Object, Text, WordPair, IntWritable> {

    TokenizerMapper(){}
    private WordPair wordPair = new WordPair();
    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	  String[] lines = value.toString().split("\\r?\\n");
	  for(int l=0;l<lines.length;l++)
        {

		String line = lines[l].toString().replaceAll("http.*?\\s|https.*?\\s"," ");
		String tokens[] = line.replaceAll("[^A-Za-z\\s]+","").split("\\s+");
		if(tokens.length > 2)
		{
		  for (int i = 0; i < tokens.length-1; i++)
		  {   if(tokens[i].contains("https")) continue;
                      if(tokens[i].length()<2) continue;
		      wordPair.setWord(tokens[i]);
		      for (int j = i+1; j < tokens.length; j++)
		      {
		          wordPair.setNeighbor(tokens[j]);
		          
			  context.write(wordPair, ONE);
		      }
		  }
		}
        }
 }
}

public static class IntSumReducer
       extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(WordPair key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word cooccurrence");
    job.setJarByClass(WordCooccurrencePairs.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(WordPair.class);
    job.setOutputKeyClass(WordPair.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
