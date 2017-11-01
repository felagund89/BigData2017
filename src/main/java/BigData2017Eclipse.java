package main.java;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLDecoder;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/*
 * argomenti
 * -m 4 -r 2 /in.txt /out
 * 
 * 
 */


public class BigData2017Eclipse extends Configured implements Tool{

	static int printUsage() {
		System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	public static	int count =0;

	public static void main(String[] args) throws Exception {
		
		try{
		
		 BigData2017Eclipse eclipseRunning = new BigData2017Eclipse();
		 int exitCode = ToolRunner.run(eclipseRunning, args);
		 System.exit(exitCode);
		}catch(Exception e){
			e.printStackTrace();
		}
		
//
//		Configuration conf = new Configuration();
//		
//		
//		try{
//			
//			conf.setInt("mapreduce.job.maps", 4);
//			conf.setInt("mapreduce.job.reduces", 2);
//	
//			
//	
//			Path input = new Path("/home/felagund/Scrivania/BIGDATA PROJECT/RDF/prova");
//	//		Path input = new Path("/home/biar/Desktop/wordcountNew/rdfFiles/prova.txt");
//			Path output =new Path("/home/felagund/Scrivania/BIGDATA PROJECT/RDF/out");
//			Job job = Job.getInstance(conf);
//	        job.setJarByClass(BigData2017Eclipse.class);
//	        job.setJobName("BigData2017");
//
//		    FileInputFormat.addInputPath(job, input);
//		    FileOutputFormat.setOutputPath(job, output);
//	
//		    job.setMapperClass(MyMapper.class);
//		    //job.setCombinerClass(BigData2017$MyReducer.class);
//		    job.setReducerClass(MyReducer.class);
//	
//	        // An InputFormat for plain text files. 
//	        // Files are broken into lines. Either linefeed or carriage-return are used 
//	        // to signal end of line. Keys are the position in the file, and values 
//	        // are the line of text.
//		    job.setInputFormatClass(TextInputFormat.class);
//	
//		    job.setOutputKeyClass(Text.class);
//		    job.setOutputValueClass(IntWritable.class);
//			//System.out.println("111" );
//		    
//
//		    job.waitForCompletion(true);
//		    System.out.println("num reduce task: "+job.getNumReduceTasks());
//			System.out.println("reduce progress: "+ job.reduceProgress());
//			System.out.println("job status: "+ job.getStatus().toString());		    
//		}catch(Exception e){
//			
//			e.printStackTrace();
//		}

	}
	
	
	public static String setSubject() {
		
		
		return null;
		
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
				String subject ;
				String predicate;
				String object;

				word.set(scanner.next());
				//prendo la riga intera, cerco di splittarla in qualche modo per creare un oggetto BasicTuple
				BasicTuple bTuple = new BasicTuple();
				String tuple = word.toString();
				
				//vanno distinti i vari  casi 4 oggetti nella riga, oppure blank node per soggetto e oggetto
				String [] splittedTuple = tuple.split("<"); 
				
				//decodifico tutto in utf8
				for(int k=0;k<splittedTuple.length;k++){
					splittedTuple[k]=URLDecoder.decode(splittedTuple[k], "UTF-8");
				}
				
				if(splittedTuple[0].equals("")){   //se splittedTuple[0] è vuoto significa che il soggetto è il successivo 			
					
					
						subject = splittedTuple[1].substring(0, splittedTuple[1].length()-2);
					
					
					predicate = splittedTuple[2];
					String [] splitPredicate = predicate.split(">");
					
					predicate=splitPredicate[0];
					object=splitPredicate[1];

					if(!object.equals(" ")){
						Boolean checkDoubleQuote = object.contains("\"");
						if(checkDoubleQuote){
							object= object.substring(object.indexOf("\"")+1, object.lastIndexOf("\""));
						}
						else{
							String objectTemp= object.replace("x", "%");
							objectTemp = objectTemp.substring(objectTemp.indexOf("%%")+2);
							objectTemp = URLDecoder.decode(objectTemp, "UTF-8");
							
							Boolean checkObjectDescribe = objectTemp.contains("DESCRIBE");
							
							if(checkObjectDescribe){
								object=objectTemp.substring(objectTemp.lastIndexOf("DESCRIBE")+12, objectTemp.length()-4);
							}
							else{
								object = objectTemp;
							}
	
						}
					}
					else{
						object = splittedTuple[3].substring(0,splittedTuple[3].indexOf(">"));
					}

					
					

				}
				else{ // soggetto=blank node , decodifico per sapere quale è 
					
					String a = splittedTuple[0].replace("x", "%");
					a = a.substring(a.indexOf("%%")+2);
					a = URLDecoder.decode(a, "UTF-8");
					Boolean checkPred = a.contains("DESCRIBE");
					
					if(checkPred){
						subject=a.substring(a.lastIndexOf("DESCRIBE")+12, a.length()-4);
					}
					else{
						subject = a;
					}
					//System.out.println("tuple " +tuple);
					predicate = splittedTuple[1];
					object="";
				//	System.out.println("subject " +subject);

				//	System.out.println("predicate " +predicate);
					String [] splitPredicate2 = predicate.split(">");
					predicate=splitPredicate2[0];

					//System.out.println("predicateSplit2 " +splitPredicate2[0]);
					
					object=splitPredicate2[1];
					if(!object.equals(" ")){
						Boolean checkDoubleQuote2 = object.contains("\"");
						if(checkDoubleQuote2){
							object= object.substring(object.indexOf("\"")+1, object.lastIndexOf("\""));
						}
						else{
							String objectTemp2= object.replace("x", "%");
							objectTemp2 = objectTemp2.substring(objectTemp2.indexOf("%%")+2);
							objectTemp2 = URLDecoder.decode(objectTemp2, "UTF-8");
							
							Boolean checkObjectDescribe2 = objectTemp2.contains("DESCRIBE");
							
							if(checkObjectDescribe2){
								object=objectTemp2.substring(objectTemp2.lastIndexOf("DESCRIBE")+12, objectTemp2.length()-4);
							}
							else{
								object = objectTemp2;
							}
							
					//		System.out.println("objectTemp "+objectTemp2);

						}
					//	System.out.println("object "+object);
					}else{
						object = splittedTuple[2].substring(0,splittedTuple[2].indexOf(">"));
//						System.out.println("object3 "+object);

					}


				}
				
				//graphlabel sono gli edge del grafo, se puntano a loro stessi non sono da contare come edge penso
				
				String graphLabel ;
				int lastindex=splittedTuple.length-1;
				Boolean checkgraphLabel = splittedTuple[lastindex].contains("DESCRIBE");
				
				if(checkgraphLabel){
					graphLabel=splittedTuple[lastindex].substring(splittedTuple[lastindex].lastIndexOf("DESCRIBE")+10, splittedTuple[lastindex].length()-4);
				}
				else{
					graphLabel = splittedTuple[lastindex].substring(0, splittedTuple[lastindex].length()-3);
				}

					



				//setto le componenti nell'oggetto basicTuple dopo che ho splittato
				bTuple.setSubject(subject);
				bTuple.setObject(object);
				bTuple.setPredicate(predicate);
				bTuple.setgraphLabel(graphLabel);

				Text text1 = new Text(subject);
				count ++;
				System.out.println(count);
//				System.out.println("tupla: "+ word.toString());
//				System.out.println("subject:  "+text1);
//				System.out.println("predicate  "+predicate);
//				System.out.println("object  "+object);
//				System.out.println("graphLabel  "+graphLabel);

				
				listBasicTuples.add(bTuple);
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
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			System.out.println("CLEANUP REDUCE:");

			super.cleanup(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("REDUCE:");
			int sum = 0;
			for (IntWritable value : values) {
				System.out.println("value: "+ value.get());
				System.out.println("key: "+key);
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
	
	
	public static class BasicTuple implements Serializable{

		/**
		 * 
		 */
		private static final long serialVersionUID = 7356936525061749503L;

		
		String subject;
		
		String object;
		
		String predicate;
		
		String optional;
		
		String graphLabel;

		public String getSubject() {
			return subject;
		}

		public void setSubject(String subject) {
			this.subject = subject;
		}

		public String getObject() {
			return object;
		}

		public void setObject(String object) {
			this.object = object;
		}

		public String getPredicate() {
			return predicate;
		}

		public void setPredicate(String predicate) {
			this.predicate = predicate;
		}

		public String getOptional() {
			return optional;
		}
 
		public void setOptional(String optional) {
			this.optional = optional;
		}
		public String getgraphLabel() {
			return graphLabel;
		}
 
		public void setgraphLabel(String graphLabel) {
			this.graphLabel = graphLabel;
		}
		
	}



	@Override
	public int run(String[] arg0) throws Exception {
		

		Configuration conf = new Configuration();
		
			conf.setInt("mapreduce.job.maps", 4);
			conf.setInt("mapreduce.job.reduces", 2);
	
			
	
			Path input = new Path("/home/felagund/Scrivania/BIGDATA PROJECT/RDF/prova");
//			Path output =new Path("/home/felagund/Scrivania/BIGDATA PROJECT/RDF/out");
//			Path output =new Path("/home/felagund/Scrivania/BIGDATA PROJECT/RDF/out");
			Job job = Job.getInstance(conf);
	        job.setJarByClass(BigData2017Eclipse.class);
	        job.setJobName("BigData2017Eclipse");

		    FileInputFormat.addInputPath(job, input);
		    FileOutputFormat.setOutputPath(job,new Path(arg0[0]));
	
		    job.setMapperClass(MyMapper.class);
		    //job.setCombinerClass(BigData2017$MyReducer.class);
		    job.setReducerClass(MyReducer.class);
	
	      
		    job.setInputFormatClass(TextInputFormat.class);
	
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    

		    
		    System.exit(job.waitForCompletion(true) ? 0:1); 
		    boolean success = job.waitForCompletion(true);
		   
		    
		    System.out.println("num reduce task: "+job.getNumReduceTasks());
			System.out.println("reduce progress: "+ job.reduceProgress());
			System.out.println("job status: "+ job.getStatus().toString());	
		    
		    return success ? 0 : 1;

		
	}

}
