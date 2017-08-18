import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sum = 0;
    
    //Summing up the counts for each word
    for (IntWritable value : values) {
      sum += value.get();
    }
    
    if(sum>=1000 && key.toString().matches(".*\\w.*")) {
      context.write(key, new IntWritable(sum));
    }
  }
}