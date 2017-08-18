import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class WordSortMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
	public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
		//System.out.println("KEY:" + key.toString());
	  	context.write(value, key);
	  	//context.write(key, value);
    }
}