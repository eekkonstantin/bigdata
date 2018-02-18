package mapreduce;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
public class CustomMapper extends Mapper<LongWritable, Text, Text, TextArrayWritable> {

  static enum Counters { NUM_RECORDS, NUM_LINES }
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
      if (line.startsWith("REVISION")) {
        _key.set(line.split(" ")[3]);
        continue;
      } else if (line.startsWith("MAIN")) {
        String[] outlinks = line.split(" ");
        outlinks = Arrays.copyOfRange(outlinks, 1, outlinks.length);
        _value.fromArray(outlinks);
      } else
        continue;

      context.getCounter(Counters.NUM_LINES).increment(1);
    }
    context.write(_key, _value);
    // context.getCounter(Counters.NUM_BYTES).increment(value.getLength());
    context.getCounter(Counters.NUM_RECORDS).increment(1);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // ...
    super.cleanup(context);
  }
}
