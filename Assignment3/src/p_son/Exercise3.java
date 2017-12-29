package p_son;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Map.Entry;
import java.util.Set;
import java.io.FileReader;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.commons.math3.stat.clustering.Clusterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.omg.CORBA.portable.InputStream;

import java.io.*;

public class Exercise3 extends Configured implements Tool {

	private static final Charset UTF8 = null;
	
	public static enum COUNTERS {
		baskets,
		itemsets
	};

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String filename = "accidents.dat";
		int parts = 1;
		
		File dir = new File("input");
		dir.mkdir();
		
		String local_threshold = "0.4";
		String global_threshold = "0.4";
		
		int total_baskets = readSplitFiles(filename, parts);
		
		System.out.println("Total baskets " + Integer.toString(total_baskets));
		
		long startTime = System.currentTimeMillis();
		
		Job phase_1 = new Job(getConf(), "Exercise 3");
		phase_1.setJarByClass(Exercise3.class);
		phase_1.setOutputKeyClass(Text.class);
		phase_1.setOutputValueClass(IntWritable.class);
		
		phase_1.getConfiguration().set("local_threshold", local_threshold );
		
		phase_1.setMapperClass(Map_stage_1.class);
		phase_1.setReducerClass(Reduce_stage_1.class);
	    
		phase_1.setInputFormatClass(TextInputFormat.class);
		phase_1.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(phase_1, new Path("input"));
	    FileOutputFormat.setOutputPath(phase_1, new Path("temp"));
	    
	    phase_1.waitForCompletion(true);
	    
		
		Job phase_2 = new Job(getConf(), "Exercise 3");
		
		
		phase_2.setJarByClass(Exercise3.class);
		phase_2.setOutputKeyClass(Text.class);
		phase_2.setOutputValueClass(IntWritable.class);
		
		phase_2.getConfiguration().set("total", Integer.toString(total_baskets) );
		phase_2.getConfiguration().set("global_threshold", global_threshold );

		phase_2.setMapperClass(Map_stage_2.class);
		phase_2.setReducerClass(Reduce_stage_2.class);
	    
		phase_2.setInputFormatClass(TextInputFormat.class);
		phase_2.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(phase_2, new Path("input"));
	    FileOutputFormat.setOutputPath(phase_2, new Path("final"));
	    
	    phase_2.waitForCompletion(true);
			    
	    long stopTime = System.currentTimeMillis();
	    
	    long elapsedTime = stopTime - startTime;
	    
	    System.out.println(elapsedTime);
	    
	  return 0;

	}
	
	private int readSplitFiles(String filename, int parts ) throws FileNotFoundException, IOException{
		
		List<String> lines = new ArrayList<String>();
		
		try (BufferedReader br = new BufferedReader(new FileReader(filename))){
			String line = null;
			while((line = br.readLine()) != null) {
				lines.add(line);
			}
		}
		
		
		int number_of_items_in_each = (int) lines.size() / parts;
		
		int cur_items = 0;
		
		for (int part_ = 0; part_ < parts; part_++){
			
			File fout = new File("input/out_" + part_);
			FileOutputStream fos = new FileOutputStream(fout);
			
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			
			if(part_ == parts-1){
				
				while(cur_items < lines.size()){
					bw.write(lines.get(cur_items));
					bw.newLine();
					
					cur_items++;
				}
				
			}else{
				
				for(int i = 0; i < number_of_items_in_each; i++){
					bw.write(lines.get(cur_items));
					bw.newLine();
					
					cur_items++;
				}
			}
			bw.close();
		}
					
		return lines.size();
	}
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new Exercise3(), args);
	    System.exit(res);

	}
	
    // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    public static class Map_stage_1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	  
    	public String local_threshold = null;
    	
	  public void setup(Context context) throws IOException, InterruptedException{
    	  
			String filename = "input/" + ((FileSplit) context.getInputSplit()).getPath().getName();
  	    	
  	    	local_threshold = (context.getConfiguration().get("local_threshold"));
  	    	
  	    	Observer ob = new MyObserver();
  	  		
  	    	String[] args_ = {filename, local_threshold};
  	    	
  	  		try {
  				Apriori A = new Apriori(args_, ob);
  			} catch (Exception e) {
  				// TODO Auto-generated catch block
  				e.printStackTrace();
  			}
  	  		  	  		
  	  		for (String itemset : ((MyObserver) ob).getItems()){
  	  			context.write( new Text(itemset), new IntWritable(1));
  	  		}
      }
	  
	  @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {

      }

      private class MyObserver implements Observer {
  		// TODO Auto-generated method stub
  		private List<String> itemsets = new ArrayList<String>();
  		
  		
  		@Override
  		public void update(Observable o, Object arg) {
  			// TODO Auto-generated method stub
  			
  			int[] itemset = (int[]) arg;
  			if(itemset.length > 1){
  				if(!itemsets.contains(Arrays.toString((int[]) arg))){
  					itemsets.add(Arrays.toString((int[]) arg));
  				}
  			}
  			
  		}
  		
  		public List<String> getItems(){
  			return itemsets;
  		}
  	 }
      
    }
   

   
   // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Reduce_stage_1 extends Reducer<Text, IntWritable, Text, IntWritable> {
      
	   public int threshold = 1;
	   
	   @Override
	   public void setup(Context context) throws IOException, InterruptedException{

	   }
	   
	  @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
		  
		 int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
   	  	 if(sum >= threshold){
   	  		 context.write(key, new IntWritable(sum));
   	  	 }	 
   	 }
      
   }
   
   // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Map_stage_2 extends Mapper<LongWritable, Text, Text, IntWritable> {
	  
     List<String[]> items = new ArrayList<String[]>();
     HashMap<String, Integer> hmap =new HashMap<String, Integer>(); 
     
	  public void setup(Context context) throws IOException, InterruptedException{
   	  
			try (BufferedReader br = new BufferedReader(new FileReader("temp/part-r-00000"))){
				String line = null;
				while((line = br.readLine()) != null) {
					String[] parts = line.split("]");
					parts[0] = parts[0].replace("[", "");
					parts[0] = parts[0].replace(" ", "");
					items.add(parts[0].split(","));
					hmap.put(Arrays.toString(parts[0].split(",")), 0);
				}
			}
     }
	  
	  @Override
     public void map(LongWritable key, Text value, Context context)
             throws IOException, InterruptedException {
		  
		  String[] items_ = (value.toString().split("\\s+"));
		  context.getCounter(COUNTERS.baskets).increment(1);
		  
		  for (String[] itemset : items){
			  
			  String key_ = Arrays.toString(itemset);
			  if(Arrays.asList(items_).containsAll(Arrays.asList(itemset)) == true){
				  int value_ = hmap.get(key_);
				  hmap.put(key_, value_ + 1);				  
			  }
	  	  }
		 
     }
	  
	  @Override 
	  public void cleanup(Context context) throws IOException, InterruptedException{
		  
		  Set<Entry<String, Integer>> set = hmap.entrySet();
		  Iterator iterator = set.iterator();
		  while(iterator.hasNext()) {
			  Map.Entry mentry = (Map.Entry)iterator.next();
			  int i = (int) (mentry.getValue() != null ? mentry.getValue() :-1);
			  context.write(new Text(mentry.getKey().toString()), new IntWritable(i) );
		  }
	  }
   }
   
// Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Reduce_stage_2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	  
	  public double threshold = 0.0; 
	  
	  @Override
	   public void setup(Context context) throws IOException, InterruptedException{
		  threshold = (double) (Double.parseDouble(context.getConfiguration().get("global_threshold")));
	   }
	  
	  @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
		 
		 int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         
         double percentage = (double) sum / (double) (Integer.parseInt(context.getConfiguration().get("total")));

         if(percentage >= threshold ){
   	  		 context.write(key, new IntWritable(sum));
   	  		 context.getCounter(COUNTERS.itemsets).increment(1);
   	  	 }
   	  	 
   	 }
      
   }
   
}
