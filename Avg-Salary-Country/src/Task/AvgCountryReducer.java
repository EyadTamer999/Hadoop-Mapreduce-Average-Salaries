package Task;

 

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class AvgCountryReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	  
	  @Override
	  public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		  float sum = 0;
		   int count = 0;
		    for (FloatWritable f : values) {
		    	sum += f.get();
		    	count++;
		  }
		    
//		    getting the average 
		    
		    FloatWritable res = new FloatWritable(sum/count);
		   
		    float maxAvg = -1; //Note that the given data contains only positive Temp values
		    
		    
		   
		    	if (res.get() > maxAvg) {
		    		maxAvg = res.get();
		    	}
		  
		    FloatWritable res2 = new FloatWritable(maxAvg);
//          getting the max average salary for every country
		    
		    context.write(key, res2);
	  
 
	}
}

