package ax;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
  private Text _src = new Text();
  private Text _val = new Text();
  private String[] outlinks = new String[2];

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    // ...
  }

  // The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
  /**
   * Input:
   *  K: linenumber
   *  V: <Revision Record - 14lines, incl. 1 empty line at the end>
   *
   * Output:
   *  K: title
   *  V: outlink
   * Output: (if no data in MAIN)
   *  K: title
   *  V: <empty>
   */
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer lineTokenizer = new StringTokenizer(value.toString(), "\n"); // splitting string into tokens by lines
    while (lineTokenizer.hasMoreTokens()) {
      String line = lineTokenizer.nextToken();
      if (line.startsWith("REVISION"))
        _src.set(line.split(" ")[3]);
      else if (line.startsWith("MAIN")) {
        outlinks = line.split(" ");
        outlinks = Arrays.copyOfRange(outlinks, 1, outlinks.length);
      }
    }

    if (outlinks.length > 0) {
      for (String o : outlinks) {
        _val.set(o);
        context.write(_src, _val);
      }
    } else // in case [title] has no outlinks
      context.write(_src, new Text());
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // ...
    super.cleanup(context);
  }
}
