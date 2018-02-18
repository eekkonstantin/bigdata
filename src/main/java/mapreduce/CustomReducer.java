package mapreduce;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
public class CustomReducer extends Reducer<Text, TextArrayWritable, Text, IntWritable> {
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    // ...
  }

  // The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
  // Make sure that the output key/value classes also match those set in your job's configuration (see below).
  @Override
  protected void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
    ArrayList<String> unique = new ArrayList<String>();
    for (TextArrayWritable tArray : values) {
      for (String t : tArray.toStrings())
        if (!unique.contains(t))
          unique.add(t);
    }
    context.write(key, new IntWritable(unique.size()));
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // ...
    super.cleanup(context);
  }
}
