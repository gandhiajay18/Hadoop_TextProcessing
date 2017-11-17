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

public class activity3{
static HashMap<String,String> map = new HashMap<String,String>();
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
   private String result = new String();
 
    TokenizerMapper(){}
    private WordPair wordPair = new WordPair();
	@Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	  String line = value.toString();

     int count = 0;
    StringBuilder sb = new StringBuilder();
//    WordPair wordPair = new WordPair();
    	if(line.length()>2){		
		String[] parts = line.split(">");
		if(parts.length==2)		{
		String loc = parts[0].toString()+">";
		Text val = new Text(loc);
		String str = parts[1].toLowerCase().replaceAll("[^A-Za-z\\s]+","");
		String str1= str.replaceAll("j","i").replaceAll("v","u");
		String[] tokens = str1.split("\\s+");
		if(tokens.length > 2)
		{	
		  for (int i = 0; i < tokens.length-1; i++)
		  {  
		  	String first = new String();
	String second = new String();
	String third = new String();
    
		      if(tokens[i].contains("https")) continue;
	              if(tokens[i].length()<2) continue;
			
		      wordPair.setWord(tokens[i]);
			first = tokens[i];			      
		for (int j = i+1; j < tokens.length; j++)
	      {
		second = tokens[j];
		for (int k = j+1; k < tokens.length; k++)
	    {
		third = tokens[k];
		if(map.containsKey(first) && map.containsKey(second) && map.containsKey(third))
		{
		String fl = new String();
		String sl = new String();
		String tl = new String();
		Text v = new Text();
    	Text x = new Text();
		String[] f =map.get(first).toString().split(":");
		String[] s =map.get(second).toString().split(":");
		String[] t =map.get(third).toString().split(":");
		v.set(val);
	for(int q=0;q<f.length;q++)
	{
	fl = f[q];
	for(int w=0;w<s.length;w++)
	{
	sl = s[w];
	for(int e=0;e<t.length;e++)
	{
	//System.out.println("F(i) is "+f[q]);
	//System.out.println("S(j) is "+s[w]);
	//System.out.println("T(k) is "+t[e]);
	tl = t[e];
	x.set(fl+","+sl+","+tl);
	context.write(x,val);	
	}}}
	}	//end if
	else if(!map.containsKey(first) && !map.containsKey(second) && !map.containsKey(third))
	{
		String fl = new String();
		String sl = new String();
		String tl = new String();
		Text v = new Text();
    	Text x = new Text();
	
		v.set(loc);
		x.set(first+","+second+","+third);
		context.write(x,val);	
	}
	else if(map.containsKey(first) && !map.containsKey(second) && !map.containsKey(third))
	{
		String fl = new String();
		String sl = new String();
		String tl = new String();
		Text v = new Text();
    	Text x = new Text();
	
		String[] f =map.get(first).toString().split(":");
		v.set(loc);
		for(int q=0;q<f.length;q++)
	{
	fl = f[q];
	x.set(fl+","+second+","+third);
	context.write(x,val);	
	}
	}
	else if(!map.containsKey(first) && map.containsKey(second) && !map.containsKey(third))
	{
		String fl = new String();
		String sl = new String();
		String tl = new String();
		Text v = new Text();
    	Text x = new Text();
	
		String[] s =map.get(second).toString().split(":");
	//	v.set(loc);
		for(int w=0;w<s.length;w++)
	{
	sl = s[w];
	x.set(first+","+sl+","+third);
	context.write(x,val);	
	}
	}
	else if(!map.containsKey(first) && !map.containsKey(second) && map.containsKey(third))
	{
		String fl = new String();
		String sl = new String();
		String tl = new String();
		Text v = new Text();
    	Text x = new Text();
	
		String[] t =map.get(third).toString().split(":");
	//	v.set(loc);
		for(int e=0;e<t.length;e++)
	{
	tl = t[e];
	x.set(first+","+second+","+tl);
	context.write(x,val);	
	}
	}
	else if(map.containsKey(first) && map.containsKey(second) && !map.containsKey(third))
		{
			String fl = new String();
		String sl = new String();
		String tl = new String();
		Text v = new Text();
    	Text x = new Text();
	
		String[] f =map.get(first).toString().split(":");
		String[] s =map.get(second).toString().split(":");
	//	v.set(val);
	for(int q=0;q<f.length;q++)
	{
	fl = f[q];
	for(int w=0;w<s.length;w++)
	{
	sl = s[w];
	//System.out.println("F(i) is "+f[q]);
	//System.out.println("S(j) is "+s[w]);
	//System.out.println("T(k) is "+t[e]);
	x.set(fl+","+sl+","+third);
	context.write(x,val);	
	}}
	}
	else if(map.containsKey(first) && !map.containsKey(second) && map.containsKey(third))
		{
			String fl = new String();
		String sl = new String();
		String tl = new String();
		Text v = new Text();
    	Text x = new Text();
	
		String[] f =map.get(first).toString().split(":");
		String[] t =map.get(third).toString().split(":");
	//	v.set(val);
	for(int q=0;q<f.length;q++)
	{
	fl = f[q];
	for(int e=0;e<t.length;e++)
	{
	tl = t[e];
	//System.out.println("F(i) is "+f[q]);
	//System.out.println("S(j) is "+s[w]);
	//System.out.println("T(k) is "+t[e]);
	x.set(fl+","+second+","+tl);
	context.write(x,val);	
	}}
	}
else if(!map.containsKey(first) && map.containsKey(second) && map.containsKey(third))
		{
			String fl = new String();
		String sl = new String();
		String tl = new String();
		Text v = new Text();
    	Text x = new Text();
	
		String[] t =map.get(third).toString().split(":");
		String[] s =map.get(second).toString().split(":");
		//v.set(val);
	for(int w=0;w<s.length;w++)
	{
	sl = s[w];
	for(int e=0;e<t.length;e++)
	{
	tl = t[e];
	//System.out.println("F(i) is "+f[q]);
	//System.out.println("S(j) is "+s[w]);
	//System.out.println("T(k) is "+t[e]);
	x.set(first+","+sl+","+tl);
	context.write(x,val);	
	}}
	}
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
       extends Reducer<Text,Text,Text,Text> {
     private String result = new String();
    private StringBuilder sb = new StringBuilder();
    Text v = new Text();
    Text x = new Text();
    Text lemma = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
    //sb.append(val.toString());
      context.write(key,val);
      }
    }
  }

public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
     private String result = new String();
     //int count = 0;
    private StringBuilder sb = new StringBuilder();
    Text v = new Text();
    Text x = new Text();
    Text lemma = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
       int    count = 0;
             sb.setLength(0);

      for (Text val : values) {
    sb.append(val.toString());
    count++;
      }

      x.set(sb.toString()+" "+String.valueOf(count));
      context.write(key,x);
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
Job job = Job.getInstance(conf, "activity 3");
    job.setJarByClass(activity3.class);
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
