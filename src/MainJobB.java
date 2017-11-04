import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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




/**
 * @author biar
 *	2.Compute the outdegree distribution;
 *  4.Report the 10 nodes with maximum outdegree with their respective degrees.
 */public class MainJobB {
	
	public static Map<String, Integer> mapOutDegree = new HashMap<String,Integer>();
	public static HashMap<String,Integer> firstTenOutdegree = new LinkedHashMap<String,Integer>();
	
	
	public static int count=0;
	
	public static void main(String[] args) {

	}

	
	public static void runJobB(Configuration conf, Path input, Path output) {
			
		Job job;
		try {
			
			
			job = Job.getInstance(conf);

			job.setJarByClass(BigData2017.class);
			job.setJobName("JobB");
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);

			job.setMapperClass(MyMapperB.class);
			job.setReducerClass(MyReducerB.class);

			job.setInputFormatClass(TextInputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);
			System.out.println("num reduce task: " + job.getNumReduceTasks());
			System.out.println("reduce progress: " + job.reduceProgress());
			System.out.println("job status: " + job.getStatus().toString());

		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static class MyMapperB extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			try{
				Scanner scanner = new Scanner(value.toString());
	
				scanner.useDelimiter("> .\\n<|> .\\n_|\\ .\\n_|\\ .\\n<");
				Text outdegree = new Text("1");
				while (scanner.hasNext()) {
	
					word.set(scanner.next());
					String tuple = word.toString();
					HashMap<String,String> hashNode = Utility.splitTuple(tuple);
					count++;
//					System.out.println("count outdree: "+count);
					Text subject = new Text(hashNode.get("subject"));
					context.write(subject, outdegree);
					
				}
			}catch(Exception e){
				e.printStackTrace();
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

	public static class MyReducerB extends Reducer<Text, Text, Text, IntWritable> {

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			System.out.println("CLEANUP REDUCE:");

			super.cleanup(context);
		}

		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			System.out.println("REDUCE: ");
			
			
			for (Text value : values) {
				
//				String currentValue = value.toString(); 
				Integer countMap = mapOutDegree.get(key.toString());
				if ( countMap == null){
					mapOutDegree.put(key.toString(), 1);
//					context.write(key, new IntWritable(1));

				}else{
					mapOutDegree.put(key.toString(), countMap+1);

//					context.write(key, new IntWritable(countMap+1));
					
				}
				
				
				int i = 0;
				mapOutDegree = Utility.sortByValue(mapOutDegree);
				context.write(new Text("MAPPA PRIMI 10 outdegree attuali"), new IntWritable(count++));

				for (String keyText : mapOutDegree.keySet()) {
					if(i<10){
						context.write(new Text(keyText) ,new IntWritable(mapOutDegree.get(keyText)));
						i++;			
					}else{
						break;
					}
				}

			}
	
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
