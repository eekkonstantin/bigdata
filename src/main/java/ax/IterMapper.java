package ax;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {
  private Text _src = new Text();
  private Text _val = new Text();

  private String LINK_PREFIX;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    LINK_PREFIX = "links";
  }

  // The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] tokens = value.toString().split("\t");

    String title = tokens[0];
    String pr = tokens[1];
    String[] links;
    if (tokens.length > 2)
      links = tokens[2].split(",");
    else
      links = new String[0];

    _src.set(title);
    _val.set(LINK_PREFIX + "\t" + (tokens.length > 2 ? tokens[2] : " "));
    context.write(_src, _val);

    for (String link : links) {
      _src.set(link);
      _val.set(pr + "\t" + links.length);
      context.write(_src, _val);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // ...
    super.cleanup(context);
  }
}
