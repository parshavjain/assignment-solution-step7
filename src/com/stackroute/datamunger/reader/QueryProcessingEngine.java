package com.stackroute.datamunger.reader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.parser.QueryParameter;

/**
 * QueryProcessingEngine is the interface which has a method
 * executeQuery(QueryParameter queryParameter) and one method getHeader with
 * definition. Note: In java 8 onwards we can have defult implementation. The
 * methods which are common the all the implemented classes can be define using
 * 'default' This interface implemented by all the different Query Processors
 **/
public interface QueryProcessingEngine {

	public DataSet executeQuery(QueryParameter queryParameter);

	// Read the header ( i.e., firt row) from the csv file.
	default List<String> getHeader(String file) {
		List<String> header = null;
		if (null != file && !file.isEmpty()) {
			try (Stream<String> stream = Files.lines(Paths.get(file))) {

				//Method 1.
				// Reading First Line from a file.
				// Splitting line.
				// storing it to array.
				String[] strArray = stream
									.map(line -> line.split(","))
									.findFirst()
									.get();
				
				// Adding Array Element to header List.
				header = new ArrayList<String>(Arrays.asList(strArray));
				
				//Method 2.
				// Reading First Line from a file.
				// Splitting line and converting to List.
				// storing it to List.
				/*header = stream
						.map(line -> Arrays.asList(","))
						.findFirst()
						.get();	*/			

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return header;
	}

}
