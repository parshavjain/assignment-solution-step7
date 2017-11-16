package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.parser.QueryParameter;

/**
 * This class will read data from CSV file and sort based on order by field.
 * 
 */
public class CsvOrderByQueryProcessor implements QueryProcessingEngine {

	private List<String> record;
	private Filter filter;

	@Override
	public DataSet executeQuery(final QueryParameter queryParameter) {
		// read header
		final List<String> headerList = getHeader(queryParameter.getFile());
		Map<String, Integer> header = IntStream.range(0, headerList.size())
				.boxed()
				.collect(Collectors.toMap(headerList::get, Function.identity()));
		
		//Setting Header Map.
		queryParameter.setHeader(header);

		// read the remaining records
		List<List<String>> result = null;
		final DataSet dataSet = new DataSet();
		if (null != queryParameter.getFile() && !queryParameter.getFile().isEmpty()) {
			try (Stream<String> stream = Files.lines(Paths.get(queryParameter.getFile()))) {

				// Reading First Line from a file.
				// Splitting line.
				// storing it to array.
				result = stream
						.skip(1)
						 .map(line -> Arrays.asList(line.split(",")))
						 .collect(Collectors.toList());
				

			} catch (IOException e) {
				e.printStackTrace();
			}

			// Sort Records of List.
			if (null != queryParameter.getOrderByFields() && !queryParameter.getOrderByFields().isEmpty()
					&& null != result && !result.isEmpty()) {
				this.sortRecords(queryParameter, result);
			}
			dataSet.setResult(result);
			
			// filter the fields based on query parameters.
			filter = new Filter();
						
			//Setting final Records to DataSet.
			dataSet.setResult(result
			       .stream()
				   .filter(list -> filter.isRequiredRecord(queryParameter, list))
				   .map(list -> filter.filterFields(queryParameter, list))
				   .collect(Collectors.toList()));
		}
		return dataSet;
	}

	private List<List<String>> sortRecords(final QueryParameter queryParameter, final List<List<String>> result) {
		
		for (String orderByField : queryParameter.getOrderByFields()) {
			final int index = queryParameter.getHeader().get(orderByField);
			if (index != -1) {
				result.sort((list1, list2) -> {
					return list1.get(index).compareTo(list2.get(index));
				});
			}
		}
		return result;
	}

}
