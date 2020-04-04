package com.app;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.springframework.stereotype.Component;

@Component
public class FileGetter {

	public List<String> fileHtmlList = new ArrayList<>();
	
	public List<String> returnHtmlFiles(String path) {
		File root = new File(path);
		Stack<File> stack = new Stack<File>();
		stack.push(root);
		while (!stack.isEmpty()) {
			File child = stack.pop();
			if (child.isDirectory()) {
				for (File file : child.listFiles()) {
					stack.push(file);
				}
			} else if (child.isFile()) {
				String fileName = child.getPath().toString();
				if (fileName.contains(".html"))
					fileHtmlList.add(fileName);
			}
		}
		return fileHtmlList;
	}
}
