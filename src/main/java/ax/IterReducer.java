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
  private float DF;
  private String LINK_PREFIX;
  private int IterID, MAX_ITERS;

  private Text _val = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration config = context.getConfiguration();
    DF = config.getFloat("DF", 0.85f);
    LINK_PREFIX = config.get("LINK_PREFIX");
    MAX_ITERS = config.getInt("MAX_ITERS", 1);
    IterID = config.getInt("IterID", 1);
  }

  // The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
  // Make sure that the output key/value classes also match those set in your job's configuration (see below).
  /**
   * Input 1:
   *  K: title
   *  V: LINK_PREFIX \t link1,link2,....
   * Input 2:
   *  K: linkX
   *  V: PR \t count(outlinks from title)
   *
   * Output:
   *  K: title
   *  V: PR \t link1,link2,....
   * Output: (LAST ITER)
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    String pageLinks = "";
    float contributions = 0f;

    for (Text v : values) {
      String[] inValData = v.toString().split("\t| "); // LINK_PREFIX, (nullable)LINKS
      if (inValData[0].startsWith(LINK_PREFIX)) // links \t [links]
        pageLinks += (inValData.length > 1 ? inValData[1] : "");
      else { // pr \t linkCount - use to calculate score
        float pageRank = Float.parseFloat(inValData[0]);
        int linkCount = Integer.parseInt(inValData[1]);
        contributions += (pageRank / linkCount);
      }
    }

    float finalRank = (1 - DF) + (DF * contributions);
    _val.set(finalRank + (IterID == MAX_ITERS ? "" : "\t" + pageLinks));
    context.write(key, _val);
  }
}
