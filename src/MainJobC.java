import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
 * 5.Compute the percentage of triples with empty context, the percentage of triples whose
 * subject is a blank node, and the percentage of triples whose object is a blank node.
 */
public class MainJobC {

	
	
	public static float countEmpty=0;
	public static float countEmptySubject=0;
	public static float countEmptyObject=0;
	public static float countEmptyContext=0;

	public static float totNumOfTriples=0;

	
	public static void main(String[] args) {

	}

	
	public static void runJobC(Configuration conf, Path input, Path output) {
			
		Job job;
		try {
			
			
			job = Job.getInstance(conf);

			job.setJarByClass(BigData2017.class);
			job.setJobName("JobC");
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);

			job.setMapperClass(MyMapperC.class);
			job.setReducerClass(MyReducerC.class);

			job.setInputFormatClass(TextInputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);

			job.waitForCompletion(true);
			

		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static class MyMapperC extends
			Mapper<LongWritable, Text, Text, FloatWritable > {
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
				
				while (scanner.hasNext()) {
	
					word.set(scanner.next());
					String tuple = word.toString();

					HashMap<String,String> hashNode = Utility.splitTuple(tuple);
					
					Text subject = new Text(hashNode.get("subject"));
					Text object = new Text(hashNode.get("object"));
					Text contxt = new Text(hashNode.get("context"));
					

					if(subject.toString().equalsIgnoreCase("blankNode")){
						context.write(new Text("empty subject"), new FloatWritable(1));
					}
					
					if(object.toString().equalsIgnoreCase("blankNode")){
						context.write(new Text("empty object1"), new FloatWritable(1));
					}
					if(contxt.toString().equalsIgnoreCase("blankNode")){
						context.write(new Text("empty context"), new FloatWritable(1));
					}
					context.write(new Text("#ofTriples"), new FloatWritable(1));
					
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

	public static class MyReducerC extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			System.out.println("CLEANUP REDUCE:");

			super.cleanup(context);
		}

		
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			
			System.out.println("REDUCE: ");
			
			

			for (FloatWritable value : values) {
				

				if(key.toString().contains("empty")){
					countEmpty++;	
					if(key.toString().contains("subject")){
						countEmptySubject++;
					}
					else if(key.toString().contains("object1")){
						countEmptyObject++;
					}
					else
						countEmptyContext++;
						
					context.write(new Text("TotEmptySubject"), new FloatWritable(countEmptySubject));
					context.write(new Text("TotEmptyObject"), new FloatWritable(countEmptyObject));
					context.write(new Text("TotEmptyContext"), new FloatWritable(countEmptyContext));

					context.write(new Text("TotEmpty"), new FloatWritable(countEmpty));

				}
				else if(key.toString().equalsIgnoreCase("#ofTriples")){
					totNumOfTriples++;
					context.write(new Text("Total number of triples"), new FloatWritable(totNumOfTriples));

				}
				
			
			}
			
				if(totNumOfTriples != 0){
					context.write(new Text("Percentage of total blanknode: "), new FloatWritable((countEmpty*100)/totNumOfTriples));
					context.write(new Text("Percentage of total blanknode Subject: "), new FloatWritable((countEmptySubject*100)/totNumOfTriples));
					context.write(new Text("Percentage of total blanknode Object: "), new FloatWritable((countEmptyObject*100)/totNumOfTriples));
					context.write(new Text("Percentage of total blanknode Context: "), new FloatWritable((countEmptyContext*100)/totNumOfTriples));
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