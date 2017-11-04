import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.PolicySpi;
import java.util.HashMap;
import java.util.Iterator;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;


public class Utility {
	private static HashMap<Integer, String> lineKeys = new HashMap<>();

	
	private static void initKeys(){
		lineKeys.put(0, "subject");
		lineKeys.put(1, "predicate");
		lineKeys.put(2, "object");
		lineKeys.put(3, "context");
	}
	
	
	//return mappa
	public static HashMap<String, String> splitNew(String tuple) {
		
		initKeys();
		
		HashMap<String, String> result = new HashMap<>();
		NxParser nxp = new NxParser();
		Iterator<Node[]> parsedLine = nxp.parse(new ByteArrayInputStream(tuple.getBytes(StandardCharsets.UTF_8)));
		
		Node[] linenode = parsedLine.next();
		for(int i=0;i<linenode.length;i++){
			System.out.println(linenode[i].toString());
		}
		
		return result;
	}

	
	
}
