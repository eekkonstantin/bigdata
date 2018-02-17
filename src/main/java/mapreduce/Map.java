package mapreduce;

import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
  static enum Counters { INPUT_WORDS }

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

  private boolean caseSensitive = true;
  private Set<String> patternsToSkip = new HashSet<String>();
  private long numRecords = 0;
  private String inputFile;

  private void parseSkipFile(Path patternsFile) {
    try {
      BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
      String pattern = null;
      while ((pattern = fis.readLine()) != null)
        patternsToSkip.add(pattern);
      fis.close();
    } catch (IOException ioe) {
      System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : ");
      ioe.printStackTrace();
    }
  }

  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
    inputFile = conf.get("map.input.file");

    if (conf.getBoolean("wordcount.skip.patterns", false)) {
      Path[] patternsFiles = new Path[0];
      try {
        patternsFiles = DistributedCache.getLocalCacheFiles(conf);
      } catch (IOException ioe) {
        System.err.println("Caught exception while getting cached files: ");
        ioe.printStackTrace();
      }

      for (Path patternsFile : patternsFiles)
        parseSkipFile(patternsFile);
    }
  }

  public void cleanup(Context ctx) {
    patternsToSkip.clear();
  }

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = caseSensitive ? value.toString() : value.toString().toLowerCase();

    for (String pattern : patternsToSkip)
      line = line.replaceAll(pattern, "");

    StringTokenizer tokenizer = new StringTokenizer(line);

    while (tokenizer.hasMoreTokens()) {
      word.set(tokenizer.nextToken());
      context.write(word, one); // ONE K-V (K-1) entry for output from mapper.
      context.getCounter(Counters.INPUT_WORDS).increment(1);
    }

    if ((++numRecords % 100) == 0)
      context.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
  }
}
