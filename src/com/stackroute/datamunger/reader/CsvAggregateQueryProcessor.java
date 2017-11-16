package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupedDataSet;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

//This class will read from CSV file and process and return the resultSet based on Aggregate functions
//Need to use Java 8 concepts like streams and filters
//Need to use Java 8 built in classes like IntSummaryStatistics OR DoubleSummaryStatistics
public class CsvAggregateQueryProcessor implements QueryProcessingEngine {

	private DataSet dataSet;
	private List<AggregateFunction> aggregates;
	private Filter filter;
	
	@Override
	public DataSet executeQuery(QueryParameter queryParameter) {
		
		filter = new Filter();
		// read header
		final List<String> headerList = getHeader(queryParameter.getFile());
		Map<String, Integer> header = IntStream.range(0, headerList.size()).boxed()
				.collect(Collectors.toMap(headerList::get, Function.identity()));

		// Setting Header Map.
		queryParameter.setHeader(header);

		// read the remaining records
		List<List<String>> result = null;
		dataSet = new DataSet();
		if (null != queryParameter.getFile() && !queryParameter.getFile().isEmpty()) {
			try (Stream<String> stream = Files.lines(Paths.get(queryParameter.getFile()))) {

				// Reading First Line from a file.
				// Splitting line.
				// storing it to array.
				result = stream.skip(1)
							   .map(line -> Arrays.asList(line.split(",")))
							   .collect(Collectors.toList());

			} catch (IOException e) {
				e.printStackTrace();
			}

			filter = new Filter();
			dataSet.setResult(result.stream()
									.filter(list -> filter.isRequiredRecord(queryParameter, list))
									.collect(Collectors.toList()));
			
			// Sort Records of List.
			if (null != queryParameter.getOrderByFields() && !queryParameter.getOrderByFields().isEmpty()
					&& null != dataSet.getResult() && !dataSet.getResult().isEmpty()) {
				this.sortRecords(queryParameter, dataSet.getResult());
			}

			dataSet.setGroupByResult(this.calclulateGroupByWithFields(dataSet.getResult(), queryParameter));
			aggregates = new ArrayList<AggregateFunction>();
			for(String key : dataSet.getGroupByResult().keySet()) {
				List<List<String>> temp = dataSet.getGroupByResult().get(key);
				aggregates.addAll(calclulateAggregates(temp, header, queryParameter.getAggregateFunctions()));
			}
			dataSet.setAggregateFunctions(aggregates);
			
		}
		return dataSet;
	}

	/**
	 * This method is used the calculate aggregates like min, max, sum, avg, sum
	 * Used IntSummaryStatistics a java 8 built in class Used Streams and filters -
	 * a java 8 concept
	 **/
	private List<AggregateFunction> calclulateAggregates(List<List<String>> result, Map<String, Integer> header,
			List<AggregateFunction> aggregates) {
		List<AggregateFunction> finalAggregateList = null;
		if (null != result && !result.isEmpty() && null != aggregates && !aggregates.isEmpty()) {
			finalAggregateList = new ArrayList<AggregateFunction>();
			for (final AggregateFunction aggregateFunction : aggregates) {
				aggregateFunction.setAggregateFieldIndex(header.get(aggregateFunction.getField()));
				switch (aggregateFunction.getFunction()) {
				case "count":
					aggregateFunction.setResult((int) result.stream()
							.map(list -> list.get(aggregateFunction.getAggregateFieldIndex())).count());
					break;
				case "min":
					Optional<Integer> min = result.stream()
							.map(list -> Integer.parseInt(list.get(aggregateFunction.getAggregateFieldIndex())))
							.min(Integer::compare);
					if (null != min) {
						aggregateFunction.setResult(min.get());
					}
					break;
				case "max":
					Optional<Integer> max = result.stream()
							.map(list -> Integer.parseInt(list.get(aggregateFunction.getAggregateFieldIndex())))
							.max(Integer::compare);

					if (null != max) {
						aggregateFunction.setResult(max.get());
					}
					break;
				case "sum":
					int sum = result.stream().map(list -> list.get(aggregateFunction.getAggregateFieldIndex()))
							.mapToInt(Integer::parseInt).sum();
					aggregateFunction.setResult(sum);
					break;
				case "avg":
					OptionalDouble average = result.stream()
							.map(list -> list.get(aggregateFunction.getAggregateFieldIndex()))
							.mapToInt(Integer::parseInt).average();
					if (null != average) {
						aggregateFunction.setResult(average.getAsDouble());
					}
					break;
				default:
					break;
				}
				finalAggregateList.add(aggregateFunction);
			}
		}
		return finalAggregateList;
	}

	private Map<String, List<List<String>>> calclulateGroupByWithFields(List<List<String>> result,
			QueryParameter queryParameter) {
		Map<String, List<List<String>>> map = null;
		if (null != queryParameter.getGroupByFields()) {
			map = new HashMap<String, List<List<String>>>();
			for (String groupByField : queryParameter.getGroupByFields()) {
				final int index = queryParameter.getHeader().get(groupByField);
				for (List<String> record : result) {
					List<List<String>> tempList = map.get(record.get(index));
					if (null == tempList) {
						tempList = new ArrayList<List<String>>();
					}
					tempList.add(record);
					List<List<String>> fList = tempList.stream()
													   .map(list -> new Filter().filterFields(queryParameter, list))
													   .filter(list -> !list.isEmpty())
													   .collect(Collectors.toList());
					map.put(record.get(index), fList);
				}
			}
		}
		return map;
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
