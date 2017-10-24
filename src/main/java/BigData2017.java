package main.java;

import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;

import main.java.bean.BasicTuple;

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
import org.apache.hadoop.util.ToolRunner;



/*
 * argomenti
 * -m 4 -r 2 /in.txt /out
 * 
 * 
 */


public class BigData2017 {

	static int printUsage() {
		System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static void main(String[] args) throws Exception {
		
		List<String> otherArgs = new ArrayList<String>();

		Configuration conf = new Configuration();
		
		
		
		
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setInt("mapreduce.job.maps", Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setInt("mapreduce.job.reduces", Integer.parseInt(args[++i]));
				} else {
					otherArgs.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				System.exit(printUsage());
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						args[i-1]);
				System.exit(printUsage());
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (otherArgs.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
					otherArgs.size() + " instead of 2.");
			System.exit(printUsage());
		}
		
		Path input = new Path(otherArgs.get(0));
		Path output =new Path(otherArgs.get(1));
		
		Job job = Job.getInstance(conf);
        job.setJarByClass(BigData2017.class);
        job.setJobName("BigData2017");
        
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);

	    job.setMapperClass(MyMapper.class);
	    //job.setCombinerClass(BigData2017$MyReducer.class);
	    job.setReducerClass(MyReducer.class);

        // An InputFormat for plain text files. 
        // Files are broken into lines. Either linefeed or carriage-return are used 
        // to signal end of line. Keys are the position in the file, and values 
        // are the line of text.
	    job.setInputFormatClass(TextInputFormat.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    job.waitForCompletion(true);

	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
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
//			scanner.useDelimiter("> .\\n<|> .\\n_|\\ .\\n_|\\ .\\n<");

			List<BasicTuple> listBasicTuples = new ArrayList<BasicTuple>();
			scanner.useDelimiter("> .\\n<|> .\\n_|\\ .\\n_|\\ .\\n<");
			while (scanner.hasNext()) {
				word.set(scanner.next());
				//prendo la riga intera, cerco di splittarla in qualche modo per creare un oggetto BasicTuple
				BasicTuple bTuple = new BasicTuple();
				String tuple = word.toString();
				
				//vanno distinti i vari  casi 4 oggetti nella riga, oppure blank node per soggetto e oggetto
				String [] splittedTuple = tuple.split(" "); //non va bene solo lo spazio, se ci sono stringhe con spazio infatti non va.
				//una buona idea potrebbe essere quella di fare il trim della stringa, ma poi si perde evidenza di quando finisce o inizia oggetto, soggetto, predicato.
				
				//setto le componenti nell'oggetto basicTuple dopo che ho splittato
				bTuple.setSubject("splittedTuple[0]");
				bTuple.setObject("");
				bTuple.setPredicate("");
				
				
				
				listBasicTuples.add(bTuple);
				context.write(word, one);
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
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}

		@Override
		public void run(Context arg0) throws IOException, InterruptedException {
			super.run(arg0);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}
	}
}
