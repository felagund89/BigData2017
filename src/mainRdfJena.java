import java.io.InputStream;
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

	
	
	
	
	
	public static void main(String[] args) {
		
		
		
//		Dataset dataset = RDFDataMgr.loadDataset("/home/biar/Desktop/provaNew.nq");
//		Model model = dataset.getDefaultModel();
//		DatasetGraphInMemory dgm = (DatasetGraphInMemory) dataset.asDatasetGraph();
//		DatasetGraph dtGraph = dataset.asDatasetGraph();
//		System.out.println(dtGraph.getDefaultGraph());
//		System.out.println("size dataset "+dtGraph.size());
//		System.out.println("isempty" + dtGraph.isEmpty());
//		Iterator<String> quads = dataset.asDatasetGraph();
//		System.out.println(dtGraph.getDefaultGraph());
//		Model graph = ModelFactory.createModelForGraph(dtGraph.getDefaultGraph());
		
//		dtGraph.listGraphNodes().listGraphNodes();
//		Graph graph = dtGraph.getDefaultGraph();
//		ExtendedIterator<Triple> quads = graph.find();
//		while (quads.hasNext()) {
//			quads.toString();
			System.out.println("");
			
//		}
//		Model model1 = model;
//		  StmtIterator stmt = dtModel.listStatements();
//		  while (stmt.hasNext;()){
//		    Statement statement = stmt.next();
//		    Resource subject = statement.getSubject();
//		    Property pred = statement.getPredicate();
//		    RDFNode object = statement.getObject();
//		    System.out.println("soggetto "+ subject);
//		    System.out.println("pred "+ pred);
//		    System.out.println("oggetto "+ object);
//
//		    Object res = null;
//		    @SuppressWarnings("unchecked")
//		    Resource resource = object.as((Class<Resource>) res);
//		    dtModel.createResource(subject).addProperty(pred,dtModel.createResource(resource));
//		  }
////		
		
				Dataset dataset=null;
		       	try {
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
		            	
		                System.out.println("name nodo "+node.getURI());
		                
//		                dsg.add(quad);
		            }
		            
		            for (Triple triple : allTripleOfGraph) {
		            	System.out.println("SOGGETTO "+ triple.getSubject() + "isblank " +triple.getSubject().isBlank() );
		            	System.out.println("OGGETTO "+ triple.getObject() + "isblank " + triple.getSubject().isBlank());
		            	System.out.println("PREDICATO "+ triple.getPredicate()+ " isblank"+ triple.getSubject().isBlank());

										
						
					}
		            
		            dataset.commit();
		            
		        } catch ( Exception e ) {
		            e.printStackTrace(System.err);
		            dataset.abort();
		        } finally {
		            dataset.end();
		        }
//		        RDFDataMgr.write(System.out, dataset, Lang.NQUADS);
		    

			
			
			
			
			
			
			
		
	}
	
	
	
	
	
	
	
	
	
}
