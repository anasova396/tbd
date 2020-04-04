package com.app;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class AdjancencyGraph {

	Map<String, HashSet<String>> adjancencyMap;

	public AdjancencyGraph() {
		adjancencyMap = new HashMap<>();
	}

	void addEdge(String link, String insideLink) {
		if(adjancencyMap.containsKey(link)) {
			HashSet<String> list = adjancencyMap.get(link);
			list.add(insideLink);
			adjancencyMap.put(link, list);
		} else {
			HashSet<String> list = new HashSet<>();
			list.add(insideLink);
			adjancencyMap.put(link, list);
		}
	}

	void print() {
		for (Map.Entry<String, HashSet<String>> entry : adjancencyMap.entrySet()) {
			System.out.println(entry.getKey() + " -> " );
			HashSet<String> list = entry.getValue();
			for (String value : list) {
				System.out.print(value + " ");
			}
			System.out.println();
				
		}
	}
	
	void writeInFile() {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonString = gson.toJson(adjancencyMap);

        try (FileWriter file = new FileWriter("resources/adjancencyList/result.json")) {
 
            file.write(jsonString);
            file.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
	}
}