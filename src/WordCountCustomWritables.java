import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountCustomWritables {
	public static class TokenizerMapper extends Mapper<Object, TextJon, TextJon, IntJon> {
		private final static IntJon one = new IntJon(1);
		private TextJon word = new TextJon();
		
		public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<TextJon, IntJon, TextJon, IntJon> {
		private IntJon result = new IntJon();
		
		public void reduce(TextJon key, Iterable<IntJon> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntJon val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word_count_custom_writables");
		job.setJarByClass(WordCountCustomWritables.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(TextJon.class);
		job.setOutputValueClass(IntJon.class);
//		job.setInputFormatClass(FileInputFormatJon.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		(new Path(args[1])).getFileSystem(conf).delete(new Path(args[1]), true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}