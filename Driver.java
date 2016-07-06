import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver implements Tool{
	

  public static void main(String[] args) throws Exception {

	    int res = ToolRunner.run(new Driver(), args);
	    System.exit(res);
   
  }
  
  public int run(String[] args) throws Exception {
	  System.out.println("Inside main");
	  System.out.println("input path: " + args[0]);
	  System.out.println("ouput path: " + args[1]);
    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
      System.exit(-1);
    }
    
    /*
     * Instantiate a Job object for your job's configuration. 
     */

  	Configuration conf = new Configuration();
  	conf.set("mapreduce.output.textoutputformat.separator", ",");
    Job job = new Job(conf);

    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(Driver.class);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and ogs.
     */
    job.setJobName("Stub Driver");
        
    job.addCacheFile(new URI("/user/hadoop/sales/sales.txt#sales"));
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    job.setInputFormatClass(TextInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(CustomerMapper.class);
    job.setReducerClass(StateReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(3);
    job.setOutputValueClass(IntWritable.class);
    boolean success = job.waitForCompletion(true);
    return success ? 0:1;
  }

@Override
public Configuration getConf() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void setConf(Configuration arg0) {
	// TODO Auto-generated method stub
	
}
  
}

