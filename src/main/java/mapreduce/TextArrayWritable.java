package mapreduce;

public class TextArrayWritable extends ArrayWritable {
  public TextArrayWritable() {
    super(TextWritable.class);
  }
}
