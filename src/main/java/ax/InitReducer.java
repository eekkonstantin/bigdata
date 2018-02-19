package ax;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
public class InitReducer extends Reducer<Text, Text, Text, Text> {
  private StringBuilder sb;
  private float initRank;

  private Text _val = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    initRank = context.getConfiguration().getFloat("initRank", 1.0f);
  }

  // The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
  // Make sure that the output key/value classes also match those set in your job's configuration (see below).
  /**
   * Input:
   *  K: title
   *  V: outlink
   *
   * Output:
   *  K: title
   *  V: PR \t link1,link2,....
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    sb = new StringBuilder(initRank + "\t");

    ArrayList<String> all = new ArrayList<String>();
    for (Text value : values) {
      String v = value.toString();
      if (!all.contains(v)) { // checking for uniqueness
        sb.append(value.toString() + ",");
        all.add(v);
      }
    }

    sb.deleteCharAt(sb.length() - 1);
    _val.set(sb.toString());
    context.write(key, _val);
  }
}
