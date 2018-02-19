package ax;

import java.io.*;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FSDataInputStream;

public class AXRecordReader extends RecordReader<LongWritable, Text>{
  private final int LINES = 14;
  private LineReader in;
  private LongWritable key = new LongWritable();
  private Text value = new Text();
  private long start =0;
  private long end =0;
  private long pos =0;

  @Override
  public void close() throws IOException { if (in != null) in.close(); }

  @Override
  public LongWritable getCurrentKey() throws IOException,InterruptedException { return key; }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException { return value; }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end)
      return 0.0f;
    else
      return Math.min(1.0f, (pos - start) / (float) (end - start));
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)throws IOException, InterruptedException {
    FileSplit split = (FileSplit) inputSplit;
    final Path file = split.getPath();
    Configuration conf = context.getConfiguration();
    FileSystem fs = file.getFileSystem(conf);
    start = split.getStart();
    end = start + split.getLength();
    boolean skipFirstLine = false;
    FSDataInputStream filein = fs.open(split.getPath());

    if (start != 0) {
      skipFirstLine = true;
      --start;
      filein.seek(start);
    }

    in = new LineReader(filein, conf);
    if (skipFirstLine)
      start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
    this.pos = start;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    key.set(pos);
    value.clear();

    final Text lineEnding = new Text("\n");
    int newSize = 0;
    for (int i=0; i < LINES; i++) {
      Text v = new Text();
      while (pos < end) {
        newSize = in.readLine(v, Integer.MAX_VALUE ,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos), Integer.MAX_VALUE));
        value.append(v.getBytes(), 0, v.getLength());
        value.append(lineEnding.getBytes(), 0, lineEnding.getLength());

        if (newSize == 0)
          break;

        pos += newSize;

        if (newSize < Integer.MAX_VALUE)
          break;
      }
    }

    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    }
    return true;
  }
}
