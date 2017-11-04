import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
	public static HashMap<String, String> splitTuple(String tuple) {
		
		initKeys();
		
		HashMap<String, String> hashNodes = new HashMap<>();
		NxParser nxp = new NxParser();
		Iterator<Node[]> parsedLine = nxp.parse(new ByteArrayInputStream(tuple.getBytes(StandardCharsets.UTF_8)));
		
		Node[] linenode = parsedLine.next();
		for(int i=0;i<linenode.length;i++){
			String splittedStr = linenode[i].toString();
//			System.out.println(linenode[i].toString());
			if(splittedStr.startsWith("_:")){
				//caso blankNode
				hashNodes.put(lineKeys.get(i),"blankNode");

			}else{
				hashNodes.put(lineKeys.get(i), linenode[i].toString());
			}
		}
		
		return hashNodes;
	}

	
	public static Map<String, Integer> sortByValue(Map<String, Integer> unsortMap) {

        List<Map.Entry<String, Integer>> list =
                new LinkedList<Map.Entry<String, Integer>>(unsortMap.entrySet());
      
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
	
	
	
}
