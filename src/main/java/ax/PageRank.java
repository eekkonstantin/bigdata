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

  /** Initial PageRank for all pages */
  public static final float INIT_RANK = 1.0f;
  /** Damping factor */
  public static final float DF = 0.85f;
  /**
   * Arbitrary prefix to help IterReducer differentiate the 2 output types
   */
  public static final String LINK_PREFIX = "links";
  /** Number of times jobIter should run (default 2) */
  public int ITERS = 2;


  public int run(String[] args) throws Exception {
    if (args.length == 3) // change max iterations if set
      ITERS = Integer.parseInt(args[2]);

    System.err.println("Iter 0: Initialization...");
    if (!jobInit(args[0], args[1] + "/iter0"))
      return 1; // exit if job fails

    // set vars for iteration paths
    String in, out;
    // run iterations
    for (int i=1; i <= ITERS; i++) {
      // reset input path to latest output path, then create new output path
      in = args[1] + "/iter" + (i-1);
      out = args[1] + "/iter" + i;
      System.err.println("Iter " + i + " of " + ITERS + ": Calculation...");
      if (!jobIter(in, out, i))
        return 1; // exit if job fails
    }

    // All completed, return OK
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
  }


  /**
   * Initializes the list of page data. Revision records are fed in to
   * InitMapper. Output data after InitReducer finishes will be in this
   * format:
   *    title \t PR \t link1,link2,....
   * Where [title] is the `article_id` in the REVISION metadata, and [PR]
   * is the initial Page Rank as specified in `INIT_RANK`. The rest of the
   * data is a comma-separated list of outlinks from [title], retrieved
   * from the MAIN line in the Revision Record.
   */
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

    // 5. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
		job.getConfiguration().setFloat("initRank", INIT_RANK);

    // 6. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
		return job.waitForCompletion(true);
  }

  /**
   * Calculates PageRank based on the input from the last Reducer job.
   * Input is in the format:
   *    title \t PR \t link1,link2,....
   * The Mapper produces 2 types of output. The first is to preserve the
   * outlink data, and the second sends data needed for the reducer to
   * calculate the PageRank.
   *    Output 1:
   *     K: title
   *     V: LINK_PREFIX \t link1,link2,....
   *    Output 2:
   *     K: linkX
   *     V: PR \t count(outlinks from title)
   *
   * The Reducer then uses both output types to calculate the title's
   * score with the PageRank algorithm, using the damping factor specified
   * by `DF`. The Reducer then produces output in this format:
   *    title \t PR \t link1,link2,....
   * This is identical to jobIter's input format, thereby allowing the job
   * to be run iteratively, with new PageRank values. In the last iteration
   * (recorded by `MAX_ITERS`), the output is slightly modified in order to
   * conform to the assignment specifications:
   *    title \t PR
   */
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
