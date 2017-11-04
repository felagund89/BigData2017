import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/*
 * argomenti
 * -m 4 -r 2 /in.txt /out
 * 
 * 
 */

public class BigData2017 {

	static int printUsage() {
		System.out
				.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static int count = 0;

     

	public static void main(String[] args) throws Exception {
        // Create new file
        //String content = "This is the content to write into create file";
        

        
      
		List<String> otherArgs = new ArrayList<String>();

		Configuration conf = new Configuration();

		try {
			for (int i = 0; i < args.length; ++i) {
				try {
					if ("-m".equals(args[i])) {
						conf.setInt("mapreduce.job.maps",
								Integer.parseInt(args[++i]));
					} else if ("-r".equals(args[i])) {
						conf.setInt("mapreduce.job.reduces",
								Integer.parseInt(args[++i]));
					} else {
						otherArgs.add(args[i]);
					}
				} catch (NumberFormatException except) {
					System.out.println("ERROR: Integer expected instead of "
							+ args[i]);
					System.exit(printUsage());
				} catch (ArrayIndexOutOfBoundsException except) {
					System.out
							.println("ERROR: Required parameter missing from "
									+ args[i - 1]);
					System.exit(printUsage());
				}
			}
			if (otherArgs.size() != 2) {
				System.out.println("ERROR: Wrong number of parameters: "
						+ otherArgs.size() + " instead of 2.");
				System.exit(printUsage());
			}

			Path input = new Path(otherArgs.get(0));
			Path output = new Path(otherArgs.get(1));

			
//			MainJobA.runJobA(conf, input, output);
//			MainJobB.runJobB(conf, input, output);
			MainJobC.runJobC(conf, input, output);

			
		} catch (Exception e) {

			e.printStackTrace();
		}


	}



	

}

