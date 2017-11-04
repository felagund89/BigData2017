import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainJobA {

	public static void main(String[] args) {

	}

	
	public static void runJobA(Configuration conf, Path input, Path output) {

		Job job;
		try {
			job = Job.getInstance(conf);

			job.setJarByClass(BigData2017.class);
			job.setJobName("JobA");
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);

			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);

			job.setInputFormatClass(TextInputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.waitForCompletion(true);
			System.out.println("num reduce task: " + job.getNumReduceTasks());
			System.out.println("reduce progress: " + job.reduceProgress());
			System.out.println("job status: " + job.getStatus().toString());

		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Scanner scanner = new Scanner(value.toString());

			scanner.useDelimiter("> .\\n<|> .\\n_|\\ .\\n_|\\ .\\n<");
			while (scanner.hasNext()) {

				word.set(scanner.next());
				String tuple = word.toString();
				
				//
				HashMap<String,String> hashNode = Utility.splitTuple(tuple);
				
				Text text1 = new Text(tuple);
				
				context.write(text1, one);

			}

		}

		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			super.run(context);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			System.out.println("CLEANUP REDUCE:");

			super.cleanup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			System.out.println("REDUCE:");
			int sum = 0;
			for (IntWritable value : values) {
				System.out.println("value: " + value.get());
				System.out.println("key: " + key);
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}

		@Override
		public void run(Context arg0) throws IOException, InterruptedException {
			System.out.println("RUN REDUCE:");

			super.run(arg0);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			System.out.println("SETUP REDUCE:");

			super.setup(context);
		}
	}

}
