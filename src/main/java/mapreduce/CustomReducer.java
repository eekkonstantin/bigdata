package mapreduce;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
public class CustomReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {
  private static final float DF = 0.85f;
  private FloatWritable _rank = new FloatWritable();
  private TextArrayWritable _op = new TextArrayWritable();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    // ...
  }

  // The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
  // Make sure that the output key/value classes also match those set in your job's configuration (see below).
  @Override
  protected void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
    // float sum = 0.0f;
    // for (FloatWritable f : values)
    //   sum += f.get();

    // ArrayList<String> out = new ArrayList<String>();
    // for (FloatWritable f : values)
    //   out.add(f.toString());
    // _op.fromArray(out);
    // context.write(key, _op);

    // _rank.set((1 - DF) + (DF * sum));
    // context.write(key, _rank);
    ArrayList<String> out = new ArrayList<>();
    for (TextArrayWritable v : values) {
      for (String t : v.toStrings())
        out.add(t);
    }

    _op.fromArray(out);
    context.write(key, _op);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // ...
    super.cleanup(context);
  }
}
