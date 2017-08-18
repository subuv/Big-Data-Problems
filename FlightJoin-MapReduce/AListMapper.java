import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AListMapper extends Mapper<LongWritable,Text,Text,Text> {
	public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException {
		String line=value.toString();
		String values = "";
		String[] words=line.split(",");

		for(int i=0; i<words.length; i++) {
			if(i==0 || i==1 || i==2 || i==7 || i==8 || i==9 || i==16 || i==17) {
				values += words[i] + ",";
			}
		}
		
		context.write(new Text("A"), new Text(values));
	}
}
