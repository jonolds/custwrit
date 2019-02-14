import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountReverse {
	
	public static class TokenizerMapperReverse extends Mapper<Object, Text, TextJon, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private TextJon word = new TextJon();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducerReverse extends Reducer<TextJon,IntWritable,TextJon,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(TextJon key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static Job initializeJob(String[] args, String jobname) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		String outfolder = "output_"+ jobname;
		if(new File(outfolder).exists())
			Files.walk(Paths.get(outfolder)).sorted(Comparator.reverseOrder()).forEach(x->x.toFile().delete());
		
		Job job = Job.getInstance(conf, jobname);
		job.setJarByClass(WordCountReverse.class);
		job.setMapperClass(TokenizerMapperReverse.class);
		job.setCombinerClass(IntSumReducerReverse.class);
		job.setReducerClass(IntSumReducerReverse.class);
		job.setOutputKeyClass(TextJon.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(outfolder));
		return job;
	}
}