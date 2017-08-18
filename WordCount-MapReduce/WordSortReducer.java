import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class WordSortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	public void reduce(IntWritable key, Text values, Context context) throws IOException, InterruptedException {
		System.out.println("Reducer!!");
		context.write(new Text(values.toString()), key);
		//context.write(key, values);
	}
}
