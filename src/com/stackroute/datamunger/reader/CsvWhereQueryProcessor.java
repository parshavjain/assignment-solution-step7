package com.stackroute.datamunger.reader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.parser.QueryParameter;

/**
 *This class will read from CSV file and process and return the resultSet based on Where clause
*Filter is user defined utility class where we defined the methods related to filter fields,
* filter records.
 **/
public class CsvWhereQueryProcessor implements QueryProcessingEngine{

	private DataSet dataSet;
	private List<String> record;
	private Map<String, Integer> header;
	private Filter filter;

/**
 *This method used to read the data from csv and filter the fields with the help of  Filter class.
 **/
	public DataSet executeQuery(QueryParameter queryParameter) {

		// read header
		List<String> headerList = getHeader(queryParameter.getFile());
		header = IntStream.range(0, headerList.size())
				.boxed()
				.collect(Collectors.toMap(headerList::get, Function.identity()));
		
		//Setting Header Map.
		queryParameter.setHeader(header);

		// read the remaining records
		List<List<String>> result = null;
		dataSet = new DataSet();
		if (null != queryParameter.getFile() && !queryParameter.getFile().isEmpty()) {
			try (Stream<String> stream = Files.lines(Paths.get(queryParameter.getFile()))) {

				// Reading First Line from a file.
				// Splitting line.
				// storing it to array.
				result = stream
						 .map(line -> Arrays.asList(line.split(",")))
						 .collect(Collectors.toList());

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// filter the fields based on query parameters.
		filter = new Filter();
		
		//Setting final Records to DataSet.
		dataSet.setResult(result
			   .stream()
			   .skip(1)
			   .filter(list -> filter.isRequiredRecord(queryParameter, list))
			   .map(list -> filter.filterFields(queryParameter, list))
			   .collect(Collectors.toList()));
		
		return dataSet;
	}

}
