import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class WikipediaPageRankDriver extends Configured implements Tool {
    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WikipediaPageRankDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
    	if (args.length != 2) {
  	      System.err.println("Usage: WordCount <input path> <output path>");
  	      System.exit(-1);
  	    }

        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        Job job = Job.getInstance(conf, "job");
        job.setJarByClass(WikipediaPageRankDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(XmlInputFormat.class);
        job.setMapperClass(WikipediaPageRankMapper.class);
        job.setMapOutputKeyClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

    	return(job.waitForCompletion(true) ? 0 : 1);
    }
}
