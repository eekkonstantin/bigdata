package ax;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

public class PageRank extends Configured implements Tool {
  public static final float DF = 0.85f;
  public static final String LINK_PREFIX = "links";
  public int ITERS = 2;

  public int run(String[] args) throws Exception {
    if (args.length == 3)
      ITERS = Integer.parseInt(args[2]);

    System.err.println("Iter 0: Initialization...");
    if (!jobInit(args[0], args[1] + "/iter0"))
      return 1;

    // set iteration vars
    String in, out;
    // run iterations
    for (int i=1; i <= ITERS; i++) {
      in = args[1] + "/iter" + (i-1);
      out = args[1] + "/iter" + i;
      System.err.println("Iter " + i + " of " + ITERS + ": Calculation...");
      if (!jobIter(in, out, i))
        return 1;
    }

    // All completed, return OK
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
  }

  public boolean jobInit(String in, String out) throws Exception {
    // 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
    Job job = Job.getInstance(getConf(), "Initialization");

    // 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(PageRank.class);

    // 2. Set mapper and reducer classes
		job.setMapperClass(InitMapper.class);
		job.setReducerClass(InitReducer.class);

    // 3. Set input and output format, mapper output key and value classes, and final output key and value classes
		job.setInputFormatClass(AXInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 4. Set input and output paths; remember, these will be HDFS paths or URLs
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

    // 6. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
		return job.waitForCompletion(true);
  }

  public boolean jobIter(String in, String out, int iter) throws Exception {
    // 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
    Job job = Job.getInstance(getConf(), "Calculation");

    // 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(PageRank.class);

    // 2. Set mapper and reducer classes
		job.setMapperClass(IterMapper.class);
		job.setReducerClass(IterReducer.class);

    // 3. Set input and output format, mapper output key and value classes, and final output key and value classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 4. Set input and output paths; remember, these will be HDFS paths or URLs
		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

    // 5. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
		job.getConfiguration().setInt("IterID", iter);
		job.getConfiguration().setInt("MAX_ITERS", ITERS);
		job.getConfiguration().set("LINK_PREFIX", LINK_PREFIX);
		job.getConfiguration().setFloat("DF", DF);

    // 6. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
		return job.waitForCompletion(true);
  }
}
