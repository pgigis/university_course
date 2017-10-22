package exercise3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

public class Exercise3 extends Configured implements Tool {

	private static final Charset UTF8 = null;

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job_1 = new Job(getConf(), "Exercise 3");
		job_1.setJarByClass(Exercise3.class);
		job_1.setOutputKeyClass(Text.class);
		job_1.setOutputValueClass(CustomKeyValue_.class);
	
		job_1.setMapperClass(Map.class);
		
	    job_1.setReducerClass(Reduce.class);
	
	    job_1.setInputFormatClass(TextInputFormat.class);
	    job_1.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job_1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job_1, new Path("temp"));
	    
		MultipleOutputs.addNamedOutput(job_1, "count", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job_1, "uniqfiles", TextOutputFormat.class, Text.class, Text.class);

	    job_1.waitForCompletion(true);
	    
	    
	    
	    HashMap<String, Integer> file_count_map_id = new HashMap<String, Integer>();
	    Integer file_count_id = 0;
	    Integer where_to_start = 1;
	    
	    FileSystem hadoop_fs = FileSystem.get(getConf());
	    for(FileStatus file: hadoop_fs.listStatus(new Path("temp"))){
	    	String file_name = file.getPath().getName();
	    	
	    	
	    	if(!file_name.startsWith("count-")){
	    		continue;
	    	}
	    	int file_id = Integer.parseInt(file_name.split("-")[2]);
	    	
	    	FSDataInputStream file_csv = hadoop_fs.open(file.getPath());
	    	StringWriter writer = new StringWriter();
	    	IOUtils.copy(file_csv, writer, UTF8);
	    	int total_elem = Integer.parseInt(writer.toString().split("\\s")[1]);
	    	

	    	
	    	file_count_map_id.put(Integer.toString(file_id), where_to_start);
	    	where_to_start += total_elem;
	    }
	    
	    int total_uniq = 0;
	    
	    for(FileStatus file: hadoop_fs.listStatus(new Path("temp"))){
	    	String file_name = file.getPath().getName();
	    	
	    	
	    	if(!file_name.startsWith("uniqfiles-")){
	    		continue;
	    	}
	    	int file_id = Integer.parseInt(file_name.split("-")[2]);
	    	
	    	FSDataInputStream file_csv = hadoop_fs.open(file.getPath());
	    	StringWriter writer = new StringWriter();
	    	IOUtils.copy(file_csv, writer, UTF8);
	    	int total_elem = Integer.parseInt(writer.toString().split("\\s")[1]);
	    	
	    	total_uniq += total_elem;
	    }
	    
	    FSDataOutputStream file_to_write = hadoop_fs.create(new Path("uniqfiles"));
	    file_to_write.writeBytes("Total Uniq:  " + Integer.toString(total_uniq));
	    file_to_write.close();
	    
		Job job_2 = new Job(getConf(), "Exercise 3");
		job_2.setJarByClass(Exercise3.class);
		job_2.setOutputKeyClass(IntWritable.class);
		job_2.setOutputValueClass(Text.class);
		
		job_2.getConfiguration().set("ids_generate", Base64.encodeBase64String(SerializationUtils.serialize((Serializable) file_count_map_id)));
		
		job_2.setMapperClass(Map2.class);
		
	    job_2.setInputFormatClass(TextInputFormat.class);
	    job_2.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job_2, new Path("temp/part*"));
	    FileOutputFormat.setOutputPath(job_2, new Path(args[1]));
	    
	    job_2.waitForCompletion(true);
	    
	  
	  return 0;

	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new Exercise3(), args);
	      
	    System.exit(res);

	}
	
   // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Map extends Mapper<LongWritable, Text, Text, CustomKeyValue_> {
      private Text word = new Text();
      private final static Text file = new Text();
      private HashMap<String, String> stopwords;
      
      public void setup(Context context) throws IOException{
    	  
    	  stopwords = new HashMap<String, String>();
    	  
    	  File file = new File("stopwords.csv");
    	  List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
    	  
    	  for (String line: lines){
    		  String[] array = line.split(",");
    		  stopwords.put(array[0], array[0]);
    	  }
      }
      
      
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
      
    	  String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
    	  
    	  for (String token: value.toString().split("\\s+")) {
    		  String tmp_str = token.replaceAll("[-+.^:_,]", "");
    		  word.set(tmp_str.toLowerCase());

    		  if(stopwords.get(token.toLowerCase()) == null && token.matches("[a-zA-Z0-9]+") && !token.isEmpty()){
    			  CustomKeyValue_ tmp = new CustomKeyValue_(new Text(filename), new IntWritable(1));
	              context.write(word, tmp );
    		  }
    	  }
      }

   }
   
   // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Reduce extends Reducer<Text, CustomKeyValue_, Text, Text> {
      
	   private MultipleOutputs<Text,Text> multiple_output;
	   private int counter;
	   private int uniq_files_occurencies;
	   
	   @Override
	   public void setup(Context context) throws IOException, InterruptedException{
		   multiple_output = new MultipleOutputs<Text,Text>(context);
		   counter = 0;
		   uniq_files_occurencies = 0;
	   }
	   
	  @Override
      public void reduce(Text key, Iterable<CustomKeyValue_> values, Context context)
              throws IOException, InterruptedException {
		 int count_files_seen = 0;
         StringBuffer list_str = new StringBuffer();
         
         HashMap<String, Integer> mapper = new HashMap<String, Integer>();
         for (CustomKeyValue_ val : values) {
            
        	if(mapper.get(val.getDocument().toString()) == null){
        		mapper.put(val.getDocument().toString(), val.getOccurrence().get());
        	}else{
        		Integer prev_occurrences = mapper.get(val.getDocument().toString());
        		mapper.put(val.getDocument().toString(), val.getOccurrence().get() + prev_occurrences);
        		
        	}
         }
         for(Entry<String, Integer> e: mapper.entrySet()){
        	 list_str.append(e.getKey() + " #" + e.getValue());
             list_str.append(", ");
             count_files_seen++;
         }
         
         if(count_files_seen == 1){
        	 uniq_files_occurencies++;
         }
         count_files_seen = 0; //just in case
         
         String str = list_str.toString();
         str = str.replaceAll(", $", "");
   	  	 context.write(key, new Text(str));
   	  	 counter++;
   	 }
      
	  @Override 
	  public void cleanup(Context context) throws IOException, InterruptedException{
          multiple_output.write("count", new Text("Total"), new Text(Integer.toString(counter)));
          multiple_output.write("uniqfiles", new Text("Total"), new Text(Integer.toString(uniq_files_occurencies)));

		  multiple_output.close();
	  }
   }
   
   // Map <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
      
	  private HashMap<String, Integer> file_count_map_id = new HashMap<String, Integer>();
	  private int id_gen = 0;
	  
      public void setup(Context context) throws IOException{
    	  
    	  file_count_map_id = (HashMap<String, Integer>) SerializationUtils.deserialize(Base64.decodeBase64(context.getConfiguration().get("ids_generate")));
    	  
    	  String filename = (((FileSplit) context.getInputSplit()).getPath().getName());
    	  int file_id = Integer.parseInt(filename.split("-")[2]);
    	  id_gen = file_count_map_id.get(Integer.toString(file_id));
      }
      
      
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {

    	  for (String token: value.toString().split("\\n")) {
              
              context.write(new IntWritable(id_gen), new Text(token));
              id_gen++;
    		 
    	  }
      }

   }
   
}
