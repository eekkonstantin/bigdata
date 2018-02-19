package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MySimpleMapReduceJob extends Configured implements Tool {





	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Job job = Job.getInstance(getConf());

		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(MySimpleMapReduceJob.class);

		// 2. Set mapper and reducer classes
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);

		// 3. Set input and output format, mapper output key and value classes, and final output key and value classes
		job.setInputFormatClass(CustomInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextArrayWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 4. Set input and output paths; remember, these will be HDFS paths or URLs
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 5. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
		// DistributedCache.addCacheFile(new Path(args[++i]).toUri(), job.getConfiguration());
		// job.getConfiguration().setBoolean("wordcount.skip.patterns", true);

		// 6. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
		boolean succeeded = job.waitForCompletion(true);
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MySimpleMapReduceJob(), args));
	}
}
