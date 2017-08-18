import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		if (args.length != 3 ) {
			System.err.println ("Usage :<inputlocation1> <inputlocation2> <outputlocation> >");
			System.exit(0);
		}
		int res = ToolRunner.run(new Configuration(), new JoinDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path p1 = new Path(files[0]);
		Path p2 = new Path(files[1]);
		Path p3 = new Path(files[2]);
		FileSystem fs = FileSystem.get(c);
		
		if(fs.exists(p3)){
			fs.delete(p3, true);
		}
		
		Job job = new Job(c,"FlightJoin");
		job.setJarByClass(JoinDriver.class);
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, AListMapper.class);
		MultipleInputs.addInputPath(job, p2, TextInputFormat.class, BListMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, p3);
		
		return (job.waitForCompletion(true)?0:1);
	}
}
