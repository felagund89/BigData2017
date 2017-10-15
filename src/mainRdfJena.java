import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.base.Sys;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.store.TripleTable;
import org.apache.jena.util.FileManager;
import org.apache.jena.util.iterator.ExtendedIterator;




public class mainRdfJena {

	
	
	
	
	
	public static void main(String[] args) throws IOException {
		
					
            	DecimalFormat df = new DecimalFormat("####0.00");

				BufferedWriter outputFile = null;

				File resultFile = new File("/home/felagund/Scrivania/BIGDATA PROJECT/resultAnalyses.txt");
				
				Dataset dataset=null;
		       	try {

		       		outputFile = new BufferedWriter(new FileWriter(resultFile));
		       		outputFile.append("\nBigData-hw1-aa2016-2017\n");

		       		
		       		
		       		dataset = RDFDataMgr.loadDataset("/home/felagund/Scrivania/BIGDATA PROJECT/RDF/btc-2010-chunk-000/btc-2010-chunk-000PART.nq");
			        dataset.begin ( ReadWrite.WRITE );

		            DatasetGraph dsg = dataset.asDatasetGraph();
		            System.out.println("size del graph "+dsg.size());
		            
		            List<Triple> allTripleOfGraph = (List<Triple>) new ArrayList();
		            
		            Iterator<Node> nodes = dsg.listGraphNodes();
		            while ( nodes.hasNext() ) {
		                Node node = nodes.next();

		            	List<Triple> allTripleSubGraph = dsg.getGraph(node).find().toList();
                
		            	allTripleOfGraph.addAll(allTripleSubGraph);
		            	
//		                System.out.println("name nodo "+node.getURI());
//		                outputFile.append("\nNode : "+node.getURI());
		                
//		                dsg.add(quad);
		            }
	                outputFile.append("\n");

		            System.out.println("fine  while ( nodes.hasNext() ) ");
		            
		            outputFile.append("\nRESULT ANALYSES\n");
		            outputFile.append("\n1. Compute the number of distinct nodes and edges in the corresponding RDF graph.");
		            outputFile.append("\n  Number of distinct nodes: "+allTripleOfGraph.size());
		           
		            
		            double emptyContext=0;
	            	double objectBlankNode=0;
	            	double subjectBlankNode = 0;
	            	
	            	outputFile.append("");
	            	
		            for (Triple triple : allTripleOfGraph) {
		            	
		            	if(triple.getSubject().isBlank())
		            		subjectBlankNode++;
		            	if(triple.getObject().isBlank())
		            		objectBlankNode++;
		            	
		            	
		            	
//		            	System.out.println("SOGGETTO "+ triple.getSubject() + "isblank " +triple.getSubject().isBlank() );
//		            	System.out.println("OGGETTO "+ triple.getObject() + "isblank " + triple.getSubject().isBlank());
//		            	System.out.println("PREDICATO "+ triple.getPredicate()+ " isblank"+ triple.getSubject().isBlank());
//		            	System.out.println("");
										
						
					}
		            System.out.println("\nfine for (Triple) ");
		            outputFile.append("\n2. Compute the outdegree distribution: does it follow a power law? Plot the result in a figure.");
		            outputFile.append("\n3. Compute the indegree distribution: does it follow a power law? Plot the result in a figure.");
		            
		            
		            outputFile.append("\n4. Which are the 10 nodes with maximum outdegree, and what are their respective degrees?");
	            	outputFile.append("\n");

		            
		            
		            outputFile.append("\n5. Compute the percentage of triples with empty context, the percentage of triples whose subject is a blank node, and the percentage of triples whose object is a blank node.");
		            
		            double mediaSubjectblank = (double)allTripleOfGraph.size()/subjectBlankNode;
		            double mediaObjectblank = (double)allTripleOfGraph.size()/objectBlankNode;

		            outputFile.append("\n  Percentage Subject blank nodes: " + df.format(mediaSubjectblank)+"%");
		            outputFile.append("\n  Percentage Object blank nodes: " + df.format(mediaObjectblank)+"%");

 
		            System.out.println("fine");
		            
		            dataset.commit();
		            outputFile.close();
		            
		        } catch ( Exception e ) {
		            e.printStackTrace(System.err);
		            dataset.abort();
		            outputFile.close();
		        } finally {
		            dataset.end();
		            outputFile.close();
		        }
//		        RDFDataMgr.write(System.out, dataset, Lang.NQUADS);

	}	
}
