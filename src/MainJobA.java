import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
 *	1.Compute the number of distinct nodes and edges in the corresponding RDF graph;
 *	3.Compute the indegree distribution.
 */
public class MainJobA {

	public static HashSet<String> hashSetNodes = new HashSet<String>(); 
	public static HashSet<String> hashSetEdges = new HashSet<String>(); 
	public static HashMap<String, Integer> mapIndegree = new HashMap<>();
	
	public static int count=0;
	
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
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);
			System.out.println("num reduce task: " + job.getNumReduceTasks());
			System.out.println("reduce progress: " + job.reduceProgress());
			System.out.println("job status: " + job.getStatus().toString());

		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static class MyMapper extends
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
				Text node1 = new Text("node1");
				Text edge = new Text("edge");
				Text indegree = new Text("1");
	
				while (scanner.hasNext()) {
	
					word.set(scanner.next());
					String tuple = word.toString();
					
					//
					HashMap<String,String> hashNode = Utility.splitTuple(tuple);
//					System.out.println("dopo nxparser..");
					count++;
					System.out.println("count: "+count);

					Text subject = new Text(hashNode.get("subject"));
//					System.out.println(subject.toString());

					Text object = new Text(hashNode.get("object"));
//					System.out.println(object.toString());

					Text predicate = new Text(hashNode.get("predicate"));
//					System.out.println(predicate.toString());
//					System.out.println("------------------");

					
//					Text cont = new Text(hashNode.get("context"));
//					System.out.println(cont.toString());
	
					context.write(node1, subject);
					context.write(node1, object);
					
					

					context.write(edge, predicate);
					context.write(object, indegree);
					
//					System.out.println(subject.toString() +" " +object.toString()+" " + predicate.toString()+" "+ cont.toString());

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

	public static class MyReducer extends Reducer<Text, Text, Text, IntWritable> {

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
				
				String currentVal = value.toString();

				if(key.toString().equalsIgnoreCase("node1")){
					hashSetNodes.add(currentVal);				
					context.write(new Text("nodes"), new IntWritable(hashSetNodes.size()));

				}
				else if(key.toString().equalsIgnoreCase("edge")){
					hashSetEdges.add(currentVal);				
					context.write(new Text("edges"), new IntWritable(hashSetEdges.size()));

				}
				else{
					Integer countMap = mapIndegree.get(key.toString());
					if ( countMap == null){
						mapIndegree.put(key.toString(), 1);
						context.write(key, new IntWritable(1));

					}
					else{
						mapIndegree.put(key.toString(), countMap+1);
						context.write(key, new IntWritable(countMap+1));

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
