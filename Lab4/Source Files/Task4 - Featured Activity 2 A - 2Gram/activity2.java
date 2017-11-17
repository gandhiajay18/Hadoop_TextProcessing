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

public class activity2{
static HashMap<String,String> map = new HashMap<String,String>();
  public static class TokenizerMapper extends Mapper<Object, Text, WordPair, Text> {
   private String result = new String();
     int count = 0;
    private StringBuilder sb = new StringBuilder();
//    WordPair wordPair = new WordPair();
    
    TokenizerMapper(){}
    
	String first = new String();
	String second = new String();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	  String line = value.toString();
		if(line.length()>2){		
		String[] parts = line.split(">");
		if(parts.length==2)
		{
Text v = new Text();
    Text x = new Text();
			WordPair wordPair = new WordPair();
		String loc = parts[0].toString()+">";
		Text val = new Text(loc);
		String t = parts[1].toLowerCase().replaceAll("[^A-Za-z\\s]+","");
		String t1= t.replaceAll("j","i").replaceAll("v","u");
		String[] tokens = t1.split("\\s+");
		if(tokens.length > 3)
		{	
		  for (int i = 0; i < tokens.length-1; i++)
		  {   
		      if(tokens[i].contains("https")) continue;
	              if(tokens[i].length()<2) continue;
			
		      wordPair.setWord(tokens[i]);
			first = tokens[i];			      
		for (int j = i+1; j < tokens.length; j++)
	      {
	      	v.clear();
	      	x.clear();
		second = tokens[j];
		if(map.containsKey(first) && map.containsKey(second))
		{
		String[] f =map.get(first).toString().split(":");
		String[] s =map.get(second).toString().split(":");
	//	System.out.println("F is "+f.toString()+f.length);
	//	System.out.println("S is "+s.toString()+s.length);
		v.set(val);
		for(int q=0;q<f.length;q++)
		{
		wordPair.setWord(f[q]);
		for(int w=0;w<s.length;w++)
		{
	//	System.out.println("F(i) is "+f[q]);
	//	System.out.println("S(i) is "+s[w]);
		wordPair.setNeighbor(s[w]);
		context.write(wordPair,v);	
		}
		}
//		sb.setLength(0);
		}
	    else if(map.containsKey(first) && !map.containsKey(second))
		{
		String[] f =map.get(first).toString().split(":");
	//	String[] s =map.get(second).toString().split(":");
		v.set(loc);
		for(int q=0;q<f.length;q++)
		{
		wordPair.setWord(f[q]);
		//System.out.println("F(i) is "+f[i]);
		//System.out.println("Second is "+second.toString());
		wordPair.setNeighbor(second.toString());
		context.write(wordPair,v);	
		}
//		sb.setLength(0);
		}
	    else if(!map.containsKey(first) && map.containsKey(second))
		{
	//	String[] f =map.get(first).toString().split(":");
		String[] s =map.get(second).toString().split(":");
		v.set(loc);
		wordPair.setWord(first);
		for(int w=0;w<s.length;w++)
		{
		wordPair.setNeighbor(s[w]);
		//System.out.println("First is "+first.toString());
		//System.out.println("S[j] is "+s[j]);
		context.write(wordPair,v);	
		}
//		sb.setLength(0);
		}
	    else if(!map.containsKey(first) && !map.containsKey(second))
		{
	//	String[] f =map.get(first).toString().split(":");
	//	String[] s =map.get(second).toString().split(":");
		v.set(loc);
		//System.out.println("First is "+first.toString());
		//System.out.println("Second is "+second.toString());
		wordPair.setWord(first);
		wordPair.setNeighbor(second);
		context.write(wordPair,v);	
//		sb.setLength(0);
		}		
	      }
			  }
			}
		}
		}	
 }
}

public static class IntSumCombiner
       extends Reducer<WordPair,Text,WordPair,Text> {
     private String result = new String();
    private StringBuilder sb = new StringBuilder();
    WordPair wordPair = new WordPair();
    Text v = new Text();
    Text x = new Text();
    Text lemma = new Text();
    public void reduce(WordPair key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
    //sb.append(val.toString());
      context.write(key,val);
      }

      //x.set(sb.toString());
      
      //sb.setLength(0);
    // String words = key.toString();
    // String[] pair = words.split(",");
    // String first = pair[0];
    // String second = pair[1];
//System.out.println("First is "+first.toString());
//System.out.println("Second is "+second.toString());
    
    }
  }

public static class IntSumReducer
       extends Reducer<WordPair,Text,WordPair,Text> {
     private String result = new String();
     //int count = 0;
    private StringBuilder sb = new StringBuilder();
    WordPair wordPair = new WordPair();
    Text v = new Text();
    Text x = new Text();
    Text lemma = new Text();
    public void reduce(WordPair key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
       int    count = 0;
             sb.setLength(0);
             x.clear();

      for (Text val : values) {
    sb.append(val.toString());
    count++;
      }

      x.set(sb.toString()+" "+String.valueOf(count));
      context.write(key,x);
    // String words = key.toString();
    // String[] pair = words.split(",");
    // String first = pair[0];
    // String second = pair[1];
//System.out.println("First is "+first.toString());
//System.out.println("Second is "+second.toString());
    
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        String csvFile = "new_lemmatizer.csv";
        String line = "";
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
                    map.put(columns[0],values);
                    len++;
                }
Job job = Job.getInstance(conf, "activity 2");
    job.setJarByClass(activity2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(WordPair.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
