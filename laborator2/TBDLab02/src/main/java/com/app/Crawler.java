package com.app;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

@Component
public class Crawler {

	List<String> links;
	
	public boolean download(String link, AdjancencyGraph adjList) throws IOException {

		if(!link.contains(".html")) {
			return false;
		}
		Document document = Jsoup.connect(link).get();
		boolean noFollow = false;
		
		Elements metaElements = document.getElementsByTag("meta");
		if(!metaElements.isEmpty()) {
			for (Element meta : metaElements) {
				if(meta.hasAttr("name")) {
					if(meta.attr("name").equals("robots")) {
						String content = meta.attr("content");
						if(content.contains("noIndex")) {
							return false;
						}					
						if(content.contains("noFollow")) {
							noFollow = true;
						}
					}
				}
			}
		} 
		URL url = new URL(link);
		String protocol = url.getProtocol();
		String host = url.getHost();
		String path = url.getPath();
		String outputPath =  "resources/output/" + protocol + "/" + host + "/" + path;		
		String outputDirectory = outputPath.substring(0, outputPath.lastIndexOf("/"));
		
		File fileOutput = new File(outputDirectory);
		if(!fileOutput.exists()) {
			fileOutput.mkdirs();
		} else {
			String file = outputPath.substring(outputPath.lastIndexOf("/") + 1, outputPath.length());
			FileGetter fileGetter = new FileGetter();
			fileGetter.returnHtmlFiles(outputPath);
			if(fileGetter.fileHtmlList.contains(file)) {
				return false;
			}
		}
		
		PrintWriter writer = new PrintWriter(outputPath);	
		Elements titles = document.getElementsByTag("title");
		if(!titles.isEmpty()) {
			for (Element title : titles) {
				writer.println(title.text());
			}
		}
		
		if(!noFollow) {
			Elements linkElements = document.getElementsByTag("a");
			if(!linkElements.isEmpty()) {
				for (Element linkElement : linkElements) {
					int indexOfFragment = 0;
					String element = linkElement.absUrl("href");
					indexOfFragment = element.indexOf("#");
					if(!element.isEmpty()) {
						String insideLink;
						if(indexOfFragment > 0) {
							insideLink =element.substring(0, indexOfFragment);
						} else {
							insideLink = element;
						}
						links.add(insideLink);
						adjList.addEdge(link, insideLink);
					}				
				}
			}
		}
		
		
		Elements htmlElements = document.getElementsByTag("body");
		if(!htmlElements.isEmpty()) {
			for (Element htmlElement : htmlElements) {
				writer.println(htmlElement.text());
			}
		}
		
		writer.close();
		return true;
	}
}
