package Task;
 

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
public class AvgSalaryMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	  
	  private static final int MISSING = -1;

	  @Override
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		  /*split using ","
		   * cast salary to float
		   * job as string (text), temp as floatWritable
		   * write into context
		   */
		  String line = value.toString();		  
		  
		  String data [] = line.split(",");
		  
		  float temp = Float.parseFloat(data[2]);
		  
		  FloatWritable salary = new FloatWritable(temp);//data for salary
		  Text job = new Text(data[1]);//data for job
		  context.write(job, salary);
		  
	  }
	}