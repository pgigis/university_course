package mahout;

import org.apache.mahout.fpm.pfpgrowth.fpgrowth.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.util.*;

import javax.security.auth.login.Configuration;

import org.apache.mahout.common.iterator.*;
import org.apache.mahout.fpm.pfpgrowth.convertors.*;
import org.apache.mahout.fpm.pfpgrowth.convertors.integer.*;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.*;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

class FPGrowthDemo {

  public static void main(String[] args) throws IOException {
    
	long minSupport = 1; //ss(long) 0.8;
    
    int aver_local = (int) ((int) 339898 * 0.08) ;
    int k = 50;
    FPGrowth<String> fps = new FPGrowth<String>();
    Collection<String> features = new HashSet<String>();
    long startTime = 0;
    try {
    
    
	
		
	features.add("17");

	   
	startTime = System.currentTimeMillis();
	
	
    } catch (Exception e) {
        e.printStackTrace();
      }
    
    StatusUpdater updater = new StatusUpdater() {
		public void update(String status){
			System.out.println("updater: " + status);
		}
	};
    
	Writer writer = null;
	String pattern = " ";

	
	StringRecordIterator iter = new StringRecordIterator(new FileLineIterable(new File("input/accidents.dat"), false), pattern);
	
	fps.generateTopKFrequentPatterns(
		iter, // use a "fresh" iterator
        fps.generateFList(iter, 1),
        (long)0.1, 
        50, 
        features, 
        new StringOutputConverter(new SequenceFileOutputCollector<Text, TopKStringPatterns>(writer)),
        updater
        );
	
	long stopTime = System.currentTimeMillis();
 
	long elapsedTime = stopTime - startTime;
    
    System.out.println(elapsedTime);
  }
}