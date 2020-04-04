package com.app;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class MapReduce {

	public void map(Map<String, HashSet<String>> adjancencyMap) throws IOException {
		for (Map.Entry<String,HashSet<String>> entry : adjancencyMap.entrySet()) {
    		String srcLink = entry.getKey();
    		HashSet<String> insideLinks = adjancencyMap.get(srcLink);

    		for (String insideLink : insideLinks) {
    			String encodedNode = encode(insideLink);
    			String encodedLink = encode(srcLink);
    			StringBuilder sb = new StringBuilder();
    			sb.append("resources/map/").append(encodedNode).append("_").append(encodedLink);
    			File file = new File(sb.toString());
    			if (!file.exists()) {
    				new FileOutputStream(file).close();
    			}
    		}
    	}
	}

	public List<String> reduce(String searchedLink) {
		String encodedSearchedLink = encode(searchedLink);
		File dir = new File("resources/map/");
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(encodedSearchedLink);
			}
		};
		String[] encodedFiles = dir.list(filter);
		if (encodedFiles == null) {
			System.out.println("Either dir does not exist or is not a directory");
		} else {
			for (int i = 0; i < encodedFiles.length; i++) {
				String filename = encodedFiles[i];
				System.out.println(filename);
			}
		}
		List<String> parentLinks = new ArrayList<>();
		for (String file : encodedFiles) {
			String encodedParentEncryptedPage = file.split("_")[1];
			parentLinks.add(decode(encodedParentEncryptedPage));
		}
		return parentLinks;

	}

	public String encode(String originalInput) {
		return Base64.getEncoder().encodeToString(originalInput.getBytes()).replace("/", ".");
	}

	public String decode(String encodedString) {
		byte[] decodedBytes = Base64.getDecoder().decode(encodedString.replace(".", "/"));
		String decodedString = new String(decodedBytes);
		return decodedString;
	}
}
