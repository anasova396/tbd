package com.app;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class App 
{
	
	private static final int MAX_PAGES = 5;
    public static void main( String[] args ) throws IOException
    {
    	boolean ok = true;

    	Crawler crawler = new Crawler();
    	AdjancencyGraph adjGraph = new AdjancencyGraph();
    	MapReduce mapReduce = new MapReduce();
        crawler.links = new ArrayList<String>();
        List<String> links = crawler.links;
        links.add("https://docs.oracle.com/javase/7/docs/api/java/util/Queue.html");
        
        for (int i = 0; i < links.size(); i++) {
        	 if(i < MAX_PAGES) {
        		ok = crawler.download(links.get(i), adjGraph);
        		if(!ok) {
        			continue;
        		}         		    	      		
			} else {
				break;
			}
        }
        adjGraph.writeInFile();
        
        mapReduce.map(adjGraph.adjancencyMap);
        String target = "https://docs.oracle.com/javase/7/docs/api/java/util/package-summary.html";
    	List<String> reffererPages = mapReduce.reduce(target);
    	System.out.println("< "+ target + ": ");
    	for(String reffererPage: reffererPages) {
    		System.out.println(reffererPage + ", ");
    	}
    	System.out.println(" >");
        
    }
}

