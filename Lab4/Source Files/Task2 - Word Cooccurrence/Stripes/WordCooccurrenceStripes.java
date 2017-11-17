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

public class WordCooccurrenceStripes{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, myMapWritable> {

    TokenizerMapper(){}
   private myMapWritable map = new myMapWritable();
   private Text word = new Text();

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
		  {   
                      if(tokens[i].length()<2) continue;
		      word.set(tokens[i]);
		      map.clear();
		      for (int j = i+1; j < tokens.length; j++)
		      {
		         if(map.containsKey(tokens[j]))
		         {
		         IntWritable occ = (IntWritable)map.get(tokens[j]);
			 occ.set(occ.get()+1);
		         }
	                 else
	                 {
			 Text word = new Text(tokens[j]);
			 map.put(word, new IntWritable(1));
			 }
                      }
                	  context.write(word, map);
		}
        }}
 }
}

public static class IntSumReducer
       extends Reducer<Text,MapWritable,Text,myMapWritable> {
    private myMapWritable finalmap = new myMapWritable();

    public void reduce(Text key, Iterable<MapWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      finalmap.clear();
      for (MapWritable val : values)
      {
       Set<Writable> words = val.keySet();
        for (Writable word : words)
        {
            IntWritable count = (IntWritable) finalmap.get(word);
            
            if (finalmap.containsKey(word))
	    {
                IntWritable updatecount = (IntWritable) finalmap.get(word);
               // System.out.println("Word is "+word+" Count is "+count+" Count get is "+count.get()+" Update count is "+updatecount);
                updatecount.set(updatecount.get() + count.get());
            }
            else
            {   IntWritable initialCount = new IntWritable(1);
		//System.out.println("Word is "+word+"Count is "+count);
                finalmap.put(word, initialCount);
            }
        }
      }
      context.write(key, finalmap);
    }
 }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word cooccurrence");
    job.setJarByClass(WordCooccurrenceStripes.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(myMapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
