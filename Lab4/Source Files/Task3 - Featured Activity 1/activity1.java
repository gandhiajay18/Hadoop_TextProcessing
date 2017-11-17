import java.io.IOException;
import java.util.StringTokenizer;
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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class activity1 {
  
static HashMap<String,String> lemmamap = new HashMap<String,String>();
  
public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
		if(line.length()>2)
		{
		String[] parts = line.split(">");
		if(parts.length==2)
		{
		String loc = parts[0].toString()+">";
          	Text val = new Text(loc);
		String t = parts[1].toLowerCase().replaceAll("[^A-Za-z\\s]+","");
		String t1= t.replaceAll("j","i").replaceAll("v","u");
		String[] tokens = t1.split("\\s+");
		  for (int i = 0; i < tokens.length; i++)
		  {  	
			 Text keys = new Text(tokens[i]);
       if(tokens[i].equals("\\s")) continue;
			context.write(keys,val);
		  }
		}
		}
    }
  }

  public static class IntSumCombiner extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
context.write(key,val);
      }

}
  }



  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    private String result = new String();
    private StringBuilder sb = new StringBuilder();
    Text v = new Text();
    Text x = new Text();
int count = 0;
    Text lemma = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
count = 0;
      for (Text val : values) {
//	result = result+val.toString();
sb.append(val.toString());
count++;
      }
      if(lemmamap.containsKey(key))
      { 
//	Text v = new Text(result);
	v.set(sb.toString()+" "+count);
	context.write(key,v);
	String vals = lemmamap.get(key);
        String[] lemmas = vals.toString().split(":");
	for(int i=0;i<lemmas.length-1;i++)
	{
//	 Text lemma = new Text(lemmas[i]);
	lemma.set(lemmas[i]);
	 context.write(lemma,v);
	}
	sb.setLength(0);
      }
	else
	{
//	 Text x = new Text(result);
         x.set(sb.toString()+" "+count);
	 context.write(key, x);
	 sb.setLength(0);
	}

}
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        String csvFile = "new_lemmatizer.csv";
        String line = "";
        //ArrayList<String> values = new ArrayList<>();
        int len=0;
        BufferedReader br = new BufferedReader(new FileReader(csvFile));

            while ((line = br.readLine()) != null)
		{
                String[] columns = line.split(",");
                    String values = "";
                    int i=1;
                    while(i<columns.length)
                    {
                        values = values+columns[i]+":";
                        i++;
                    }
                    lemmamap.put(columns[0],values);
                    len++;
                }
//System.out.println("Map size is "+lemmamap.size());
	
    Job job = Job.getInstance(conf, "activity 1");
    job.setJarByClass(activity1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
