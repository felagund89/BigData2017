//package main.java.old;
//
//import java.io.File;
//
//import main.java.old.NodeCountReducer;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.jena.hadoop.rdf.io.input.nquads.NQuadsInputFormat;
//import org.apache.jena.hadoop.rdf.io.output.ntriples.NTriplesNodeOutputFormat;
//import org.apache.jena.hadoop.rdf.types.NodeWritable;
//
//import com.google.common.io.Files;
//
//public class RdfMapReduceExample {
//
//	public static final String PATH_FILE ="/home/felagund/Scrivania/BIGDATA PROJECT/RDF/btc-2010-chunk-000/btc-2010-chunk-000.nq";
//	public static final String PATH_FILE_PROVA ="/home/felagund/Scrivania/BIGDATA PROJECT/RDF/prova";
//	public static final String PATH_FILE_OUTPUT ="/home/felagund/Scrivania/BIGDATA PROJECT/RDF/resultAnalyses";
//
//	
//    public static void main(String[] args) {
//        try {
//        	
//        	//TODO: cancellare cartella resultAnalyses
//        	/*File index = new File("/home/biar/Desktop/resultAnalyses");
//        	String[]entries = index.list();
//        	for(String s: entries){
//        	    File currentFile = new File(index.getPath(),s);
//        	    currentFile.delete();
//        	} */
//        	
//            // Get Hadoop configuration
//            Configuration config = new Configuration(true);
//            
//
//            // Create job
//            Job job = Job.getInstance(config);
//            job.setJarByClass(RdfMapReduceExample.class);
//            job.setJobName("RDF Quad Node Usage Count");
//
//            // Map/Reduce classes
//            //job.setMapperClass(TripleNodeCountMapper.class);
//            job.setMapperClass(QuadNodeCountMapper.class);
//            job.setMapOutputKeyClass(NodeWritable.class);
//            job.setMapOutputValueClass(LongWritable.class);
//            job.setReducerClass(NodeCountReducer.class);
//
//            // Input and Output
//            //job.setInputFormatClass(TriplesInputFormat.class);
//            
//            job.setInputFormatClass(NQuadsInputFormat.class);
//            job.setOutputFormatClass(NTriplesNodeOutputFormat.class);
//            //job.setOutputFormatClass(NQuadsOutputFormat.class);
//            
//            FileInputFormat.setInputPaths(job, new Path(PATH_FILE));
//            FileOutputFormat.setOutputPath(job, new Path(PATH_FILE_OUTPUT));
//
//            // Launch the job and await completion
//            job.submit();
//            if (job.monitorAndPrintJob()) {
//                // OK
//                System.out.println("Completed");
//            } else {
//                // Failed
//                System.err.println("Failed: job" + job);
//            }
//        } catch (Throwable e) {
//            e.printStackTrace();
//            
//        }
//    }
//
////	public static void main(String[] args) {
////		resultAnalyses
////	}
//}