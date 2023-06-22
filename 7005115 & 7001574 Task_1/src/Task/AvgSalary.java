package Task;

 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class AvgSalary {

	  public static void main(String[] args) throws Exception {
//		  Highest paying job is in RU	157500.0
	    if (args.length != 2) {
	      System.err.println("Usage: AvgSalary <input path> <output path>");
	      System.exit(-1);
	    }

	    Job job = Job.getInstance();
	    job.setJarByClass(AvgSalary.class);
	    job.setJobName("Average Salary Per JobS");

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.setMapperClass(AvgSalaryMapper.class);
	    job.setReducerClass(AvgSalaryReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}
