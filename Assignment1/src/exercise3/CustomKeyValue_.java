package exercise3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class CustomKeyValue_ implements Writable{
	   private Text document;
	   private IntWritable occurrence;
	   
	   public CustomKeyValue_(){
		   document = new Text();
		   occurrence = new IntWritable(0);
	   }
	 
	   public CustomKeyValue_(Text document, IntWritable occurrence){
		   this.document = document;
		   this.occurrence = occurrence;
	   }
	   
	   public Text getDocument(){
		   return document;
	   }
	   
	   public IntWritable getOccurrence(){
		   return occurrence;
	   }
	   
	   @Override
	   public void write(DataOutput out) throws IOException{
		   document.write(out);
		   occurrence.write(out);
	   }

	   @Override
	   public void readFields(DataInput in) throws IOException {
		   document.readFields(in);
		   occurrence.readFields(in);
	   }
	   
	   
}