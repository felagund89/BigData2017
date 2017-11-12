import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

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
 * 
 * 
 * 6. For each triple, compute the number of distinct contexts in which the triple appears
 *	(the empty context counts as 1). Report the 10 triples with the largest number of distinct
 *	contexts.
 * 7. Remove duplicate triples (i.e., produce one or more output files in which triples have
 *	no context and each triple appears only once).
 * 
 * 
 */
public class MainJobD {

	public static int count = 0;
	public static Map<String, HashSet<String>> mapContext = new HashMap<>();
	public static Map<String, Integer> mapNumberContext = new HashMap<>();
	public static Set<String> hashblankNode = new HashSet<>();
	
	public static void main(String[] args) {

	}

	public static void runJobD(Configuration conf, Path input, Path output) {

		Job job;
		try {

			job = Job.getInstance(conf);

			job.setJarByClass(BigData2017.class);
			job.setJobName("JobD");
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);

			job.setMapperClass(MyMapperD.class);
			job.setReducerClass(MyReducerD.class);

			job.setInputFormatClass(TextInputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);
			

		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static class MyMapperD extends
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

			try {
				Scanner scanner = new Scanner(value.toString());

				scanner.useDelimiter("> .\\n<|> .\\n_|\\ .\\n_|\\ .\\n<");

				while (scanner.hasNext()) {

					word.set(scanner.next());
					String tuple = word.toString();

					HashMap<String, String> hashNode = Utility.splitTuple(tuple);

					count++;

					Text subject = new Text(hashNode.get("subject"));
					Text object = new Text(hashNode.get("object"));
					Text predicate = new Text(hashNode.get("predicate"));

					Text contx = new Text(hashNode.get("context"));

					Text tupleFinal = new Text(subject + " " + predicate + " "
							+ object);
					context.write(tupleFinal, contx);

				}
			} catch (Exception e) {
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

	public static class MyReducerD extends
			Reducer<Text, Text, Text, IntWritable> {

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			System.out.println("CLEANUP REDUCE:");

			super.cleanup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {


			for (Text value : values) {

				String currentVal = value.toString();

				HashSet<String> tempSet = mapContext.get(key.toString());

				if (tempSet == null) {
					
					if(currentVal.equalsIgnoreCase("blankNode")){     //useful for point 7
						hashblankNode.add(key.toString());          
					}
					tempSet = new HashSet<String>();
					tempSet.add(currentVal);
					mapContext.put(key.toString(), tempSet);
					mapNumberContext.put(key.toString(), 1); //first time found
				} else {
					if(currentVal.equalsIgnoreCase("blankNode")){
						hashblankNode.add(key.toString());
					}
						tempSet.add(currentVal);
						
						mapContext.remove(key.toString());
						mapNumberContext.remove(key.toString());
						
						mapContext.put(key.toString(), tempSet);
						mapNumberContext.put(key.toString(), tempSet.size());
				}

				//point 7
				context.write(new Text("\n"+"CONTEXT BLANKNODE"), new IntWritable(hashblankNode.size()));
				String[] blankNodeArray = hashblankNode.toArray(new String[hashblankNode.size()]);
				for (int i = 0; i < blankNodeArray.length; i++) {
					context.write(new Text(blankNodeArray[i]) ,new IntWritable(0));
				}
				
				//point 6
				
				int i = 0;
				mapNumberContext = Utility.sortByValue(mapNumberContext);
				context.write(new Text("\n"+"MAPPA PRIMI 10 maggiori context"), new IntWritable(count++));
				
				for (String keyText : mapNumberContext.keySet()) {
					if(i<10){
						context.write(new Text(keyText) ,new IntWritable(mapNumberContext.get(keyText)));
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
