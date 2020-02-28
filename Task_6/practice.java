import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import java.io.IOException; 
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;   
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;

public class practice  {

public static class Map2 extends Mapper<Object, Text, Text, LongWritable> { 
  
    private TreeMap<Long, String> tmap; 
  
    @Override
    public void setup(Context context) throws IOException,InterruptedException 
    { 
        tmap = new TreeMap<Long, String>(); 
    } 
  
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    { 
  
        
        String[] tokens = value.toString().split("\t"); 

        String word = tokens[0]; 

        long no_of_words = Long.parseLong(tokens[1]); 
  
        tmap.put(no_of_words, word); 
  
        if (tmap.size() > 100) 
        { 
            tmap.remove(tmap.firstKey()); 
        } 
    } 
  
    @Override
    public void cleanup(Context context) throws IOException,InterruptedException 
    { 
    	for (Entry<Long, String> entry : tmap.entrySet())  
        { 
            long count = (long)entry.getKey(); 
            String name = (String)entry.getValue(); 
		
  
            context.write(new Text(name), new LongWritable(count)); 
            
         } 
    } 
} 

public static class Reduce2 extends Reducer<Text,LongWritable, LongWritable, Text> { 

	private TreeMap<Long, String> tmap2; 

	@Override
	public void setup(Context context) throws IOException, InterruptedException 
	{ 
		tmap2 = new TreeMap<Long, String>(); 
	} 

	@Override
	public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException 
	{ 

		String name = key.toString(); 
		long count = 0; 

		for (LongWritable val : values) 
		{ 
			count = val.get(); 
		} 

		
		tmap2.put(count, name); 

		
		if (tmap2.size() > 100) 
		{ 
			tmap2.remove(tmap2.firstKey()); 
		} 
	} 
	

	@Override
	public void cleanup(Context context) throws IOException,InterruptedException 
	{ 
		for (Entry<Long, String> entry : tmap2.entrySet())  
        { 
            long count = (long)entry.getKey(); 
            String name = (String)entry.getValue(); 
			context.write(new LongWritable(count), new Text(name)); 
		} 
	} 
}


      

public static void main (String[] args) throws Exception { 

	Configuration conf = new Configuration(); 
     

    if (args.length < 2)  
    { 
        System.err.println("Error: please provide two paths"); 
        System.exit(2); 
    } 

    Job job = Job.getInstance(conf); 
    job.setJarByClass(practice.class); 

    job.setMapperClass(Map2.class); 
    job.setReducerClass(Reduce2.class); 
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setOutputKeyClass(LongWritable.class); 
    job.setOutputValueClass(Text.class); 
    
    FileInputFormat.addInputPath(job, new Path(args[0])); 
    FileOutputFormat.setOutputPath(job, new Path(args[1])); 

    job.submit();
    job.waitForCompletion(true);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1); 

 }


}
  