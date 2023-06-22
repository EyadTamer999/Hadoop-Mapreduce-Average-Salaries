package Task;

 

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class AvgSalaryReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	  
	  @Override
	  public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
 
	    float sum = 0;
	    int count = 0;
	    for (FloatWritable f : values) {
	    	sum += f.get();// getting the sum of all the salaries
	    	count++;// getting the count of the jobs
	  }
	    
	    FloatWritable res = new FloatWritable(sum/count);//getting the average for the salaries per job

	    
	    context.write(key, res);
	  
 
	}
}

