import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
  public static final Log log = LogFactory.getLog(CustomerMapper.class);

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  log.info("inside mapper");	  
	  if (value.toString().length() > 0) {
		  String arrEntityAttributes[] = value.toString().split(",");
		  
		  String customerId = arrEntityAttributes[0];
		  String state = arrEntityAttributes[4];
		  log.info("inside mapper, the customerId is " + customerId);
		  log.info("inside mapper, the state is " + state);
		  context.write(new Text(state), new IntWritable(Integer.parseInt(customerId)));
	  }
  }
}
