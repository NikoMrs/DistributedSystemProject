package it.unitn.ds1;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class test {
	public static void main(String[] args) throws InterruptedException {

		try {
			String filename = "./logs/filename.txt";

			Path path = Paths.get(filename);
			Files.createDirectories(path.getParent());

			File logFile = new File(filename);
			if (logFile.createNewFile()) {
				System.out.println("File created: " + logFile.getName());
			}

			FileWriter myWriter = new FileWriter(filename);
			myWriter.write("Files in Java might be tricky, but it is fun enough!");
			myWriter.close();

		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

	}
}