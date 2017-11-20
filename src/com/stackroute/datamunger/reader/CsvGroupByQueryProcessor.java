package com.stackroute.datamunger.reader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

//This class will read from CSV file and process and return the resultSet based on Group by fields
//Need to use Java 8 concepts like streams and filters
//Need to use Java 8 built in classes like IntSummaryStatistics OR DoubleSummaryStatistics
public class CsvGroupByQueryProcessor implements QueryProcessingEngine {

	private DataSet dataSet;
	private List<String> record;
	private Map<String, Integer> header;
	private Filter filter;

	@Override
	public DataSet executeQuery(QueryParameter queryParameter) {

		// read header
		final List<String> headerList = getHeader(queryParameter.getFile());
		header = IntStream.range(0, headerList.size()).boxed()
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
									.filter(record -> filter.isRequiredRecord(queryParameter, record))
									.collect(Collectors.toList()));
			
			// Sort Records of List.
			if (null != queryParameter.getOrderByFields() && !queryParameter.getOrderByFields().isEmpty()
					&& null != dataSet.getResult() && !dataSet.getResult().isEmpty()) {
				this.sortRecords(queryParameter, dataSet.getResult());
			}

			//Calculate group by field.
			dataSet.setGroupByResult(this.calclulateGroupByWithFields(dataSet.getResult(), queryParameter));
			
			//Calculate group by field with aggregates.
			if (null != queryParameter.getAggregateFunctions()) {
				dataSet.setGroupByAggregateResult(new HashMap<String, DoubleSummaryStatistics>());
				for(String strings : dataSet.getGroupByResult().keySet()) {
					Map<String, DoubleSummaryStatistics> groupByAggregateResult = new HashMap<String, DoubleSummaryStatistics>();
					groupByAggregateResult = calclulateGroupByWithAggregates(dataSet.getGroupByResult().get(strings), queryParameter);
					for (String string : groupByAggregateResult.keySet()) {
						dataSet.getGroupByAggregateResult().put(strings, groupByAggregateResult.get(string));
					}
				}
			}
			dataSet.setResult(null);
		}
		return dataSet;
	}

	/**
	 * This method is used to calculate group by summary with fields. It means along
	 * with group by, some other fields also present in given query.
	 **/
	private Map<String, List<List<String>>> calclulateGroupByWithFields(List<List<String>> result,
			QueryParameter queryParameter) {
		Map<String, List<List<String>>> map = null;
		if (null != queryParameter.getGroupByFields()) {
			map = new HashMap<String, List<List<String>>>();
			for (String groupByField : queryParameter.getGroupByFields()) {
				for (List<String> record : result) {
					final int index = getGroupByFieldIndex(record, groupByField);
					List<List<String>> tempList = map.get(record.get(index));
					if (null == tempList) {
						tempList = new ArrayList<List<String>>();
					}
					tempList.add(record);
					
					map.put(record.get(index), tempList);
				}
			}
		}
		return map;
	}

	/**
	 * This method is use to get index/position of group by field
	 **/
	private int getGroupByFieldIndex(List<String> selectedFields, String groupByField) {
		
		if(null == header.get(groupByField)) {
			return 0;
		}
		
		return header.get(groupByField);
	}

	/**
	 * This method is used to calculate group by along with other aggregate
	 * functions like min, max, avg, sum, count. Used streaps and filters of java 8
	 * Used DoubleSummaryStatistics a java 8 built-in class to store aggregate
	 * functions
	 **/
	private Map<String, DoubleSummaryStatistics> calclulateGroupByWithAggregates(List<List<String>> result,
			QueryParameter queryParameter) {
		Map<String, DoubleSummaryStatistics> doubleSummaryMap = null;
		if (null != result && !result.isEmpty() && null != queryParameter.getAggregateFunctions()
				&& !queryParameter.getAggregateFunctions().isEmpty()) {
			doubleSummaryMap = new HashMap<String, DoubleSummaryStatistics>();
			for (final AggregateFunction aggregateFunction : queryParameter.getAggregateFunctions()) {
				//for (final List<String> record : result) {
					aggregateFunction
					.setAggregateFieldIndex(null == queryParameter.getHeader().get(aggregateFunction.getField()) ? 0 : queryParameter.getHeader().get(aggregateFunction.getField()));
					DoubleSummaryStatistics dSS = result.stream()
							.mapToDouble(list -> Double.parseDouble(list.get(aggregateFunction.getAggregateFieldIndex())))
							.summaryStatistics();
					doubleSummaryMap.put(String.valueOf(aggregateFunction.getAggregateFieldIndex()), dSS);
				//}

			}
		}
		return doubleSummaryMap;
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
