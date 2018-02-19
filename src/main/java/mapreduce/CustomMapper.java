package mapreduce;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
public class CustomMapper extends Mapper<LongWritable, Text, Text, TextArrayWritable> {

  static enum Counters { NUM_RECORDS, NUM_LINES }
  private Text _src = new Text();
  private FloatWritable _score = new FloatWritable();
  private String[] outlinks = new String[2];
  private Text _key = new Text();
  private TextArrayWritable _value = new TextArrayWritable();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    // ...
  }

  // The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    System.err.println(value.toString());

    StringTokenizer lineTokenizer = new StringTokenizer(value.toString(), "\n"); // splitting string into tokens by lines
      // run until 14 ; 14 lines per record
    while (context.getCounter(Counters.NUM_LINES).getValue() < 14 && lineTokenizer.hasMoreTokens()) {
      String line = lineTokenizer.nextToken();
      if (line.startsWith("REVISION"))
        _src.set(line.split(" ")[3]);
      else if (line.startsWith("MAIN")) {
        outlinks = line.split(" ");
        outlinks = Arrays.copyOfRange(outlinks, 1, outlinks.length);
        _value.fromArray(outlinks);
      }

      context.getCounter(Counters.NUM_LINES).increment(1);
    }

    context.write(_src, _value);

    // float pagerank = 1.0f;
    // _score.set(pagerank / (outlinks.length - 1));
    //
    // ArrayList<String> done = new ArrayList<String>(); // so only unique added
    // for (int i=1; i<outlinks.length; i++) {// start at 1; 0 is "MAIN"
    //   String t = outlinks[i];
    //   if (!done.contains(t)) {
    //     _key.set(t);
    //     context.write(_key, _score);
    //     done.add(t);
    //   }
    // }

    // context.write(_key, _value);
    // context.getCounter(Counters.NUM_BYTES).increment(value.getLength());
    context.getCounter(Counters.NUM_RECORDS).increment(1);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // ...
    super.cleanup(context);
  }
}
