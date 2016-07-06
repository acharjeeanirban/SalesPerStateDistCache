import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StateReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    
  public static final Log log = LogFactory.getLog(StateReducer.class);
	
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	   
	  log.info("inside reducer");
	  
	  int sum = 0;
	  for (IntWritable value : values) {
		  log.info("the int writable value is " + value.get());
		  File file = new File("./sales");
		  FileInputStream fis = new FileInputStream(file);
		  BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		  String line = null;
		  String arr[] = null;
		  while ((line = reader.readLine()) != null) {
		      arr = line.split(",");
		      log.info("The arr value is : " + arr[0] + " " + arr[1] + " " + arr[2]);
		  
			  if (Integer.parseInt(arr[1]) == value.get()) {
				  sum += Integer.parseInt(arr[2]);
			  }
		  }
		  
	  }
	  
	  context.write(key, new DoubleWritable(sum));
	  
  }
}