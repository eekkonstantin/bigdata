package ax;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
public class IterReducer extends Reducer<Text, Text, Text, Text> {
  private float df;
  private String LINK_PREFIX;

  private Text _val = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    df = 0.85f;
    LINK_PREFIX = "links";
  }

  // The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
  // Make sure that the output key/value classes also match those set in your job's configuration (see below).
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    String pageLinks = "";
    float contributions = 0f;

    for (Text v : values) {
      if (v.toString().startsWith(LINK_PREFIX)) // links \t [links]
        pageLinks += v.toString();
      else { // pr \t linkCount - use to calculate score
        String[] data = v.toString().split("\t");
        float pageRank = Float.parseFloat(data[0]);
        int linkCount = Integer.parseInt(data[1]);
        contributions += (pageRank / linkCount);
      }
    }

    float finalRank = (1 - df) + (df * contributions);
    _val.set(finalRank + "\t" + pageLinks);
    context.write(key, _val);
  }
}
