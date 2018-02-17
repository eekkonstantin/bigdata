package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;

public class TextArrayWritable extends ArrayWritable {
  public TextArrayWritable() {
    super(Text.class);
  }

  public TextArrayWritable(Text[] values) {
    super(Text.class, values);
  }

  public void fromArray(String[] values) {
    Text[] out = new Text[values.length];
    for (int i=0; i<values.length; i++)
      out[i] = new Text(values[i]);
    this.set(out);
  }
}
