package exercise1;

import java.io.InputStream;

import java.io.OutputStream;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.lang.*;
import org.apache.log4j.*;
public class Exercise1 extends Configured implements Tool {
	
	static Logger log = Logger.getLogger(Exercise1.class.getName());
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job_1 = new Job(getConf(), "Exercise1");
		job_1.setJarByClass(Exercise1.class);
		job_1.setOutputKeyClass(Text.class);
		job_1.setOutputValueClass(IntWritable.class);
	
		job_1.setMapperClass(Map_stage_1.class);
		
	    job_1.setReducerClass(Reduce_stage_1.class);
	
	    job_1.setInputFormatClass(TextInputFormat.class);
	    job_1.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job_1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job_1, new Path("temp/output"));
	
	    job_1.waitForCompletion(true);
	    
	    Job job_2 = new Job(getConf(), "Exercise1");
		job_2.setJarByClass(Exercise1.class);
		job_2.setOutputKeyClass(IntWritable.class);
		job_2.setOutputValueClass(Text.class);
		
		job_2.setMapperClass(Map_stage_2.class);
		job_2.setNumReduceTasks(1);
		job_2.setSortComparatorClass(DescendingComparator.class);
		
		job_2.setMapOutputValueClass(Text.class);
		job_2.setMapOutputKeyClass(IntWritable.class);
		
	    job_2.setReducerClass(Reduce_stage_2.class);
	
	    job_2.setInputFormatClass(TextInputFormat.class);
	    job_2.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job_2, new Path("temp/output"));
	    FileOutputFormat.setOutputPath(job_2, new Path(args[1]));
	
	    job_2.waitForCompletion(true);
	  
	  
	  return 0;

	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new Exercise1(), args);
	      
	    System.exit(res);

	}
	
   // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Map_stage_1 extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
         for (String token: value.toString().split("\\s+")) {
   		  	token = token.replaceAll("[-+.^:,]", "");

            if((token.matches("[a-zA-Z0-9]+")) && !token.isEmpty()){ 
            	word.set(token.toLowerCase());
            	context.write(word, ONE);
           }
         }
      }
   }
   
   // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Reduce_stage_1 extends Reducer<Text, IntWritable, Text, IntWritable> {
     private int k = 10;
	  	protected void setup(Reducer.Context context)
		      throws IOException,
		        InterruptedException {
		      Configuration config = context.getConfiguration();
		      this.k = config.getInt("exercise1.case.k", 10);
	   	}
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
   	  	 if(sum >= 4000){
   	  		 context.write(key, new IntWritable(sum));
   	  	 }
      }
   }
   
   // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Map_stage_2 extends Mapper<LongWritable, Text, IntWritable, Text> {
	      
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  	  
    	   		String[] token = value.toString().split("\\s");

             	context.write( new IntWritable(Integer.parseInt(token[1])), new Text(token[0]));
      }
   }
   
   
   // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Reduce_stage_2 extends Reducer<IntWritable, Text, IntWritable, Text> {
	  private int k;
	  protected void setup(Reducer.Context context)
		      throws IOException,
		        InterruptedException {
		      Configuration config = context.getConfiguration();
		      this.k = config.getInt("Exercise1.case.k", 10);
		    }
	  @Override
	  public void reduce(IntWritable key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  	  
		  	  String value_str = "";
		  	  for (Text val: values){
		  		  value_str += val.toString();
		  	  }
    	  	  if(this.k >= 0){
    	  		  String tmp = key.get() + " " + value_str;
    	  		  System.out.println(tmp);
    	  		  this.k -= 1;
    	  	  }
    		  context.write(key, new Text(value_str));
    	  
      }
   }
   
   public static class DescendingComparator extends WritableComparator {
	   protected  DescendingComparator() {
	        super(IntWritable.class, true);
	    }
	   @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
		    IntWritable key1 = (IntWritable) w1;
	        IntWritable key2 = (IntWritable) w2;          
	        return -1 * key1.compareTo(key2);
	    }
   }

}
