package com.stackroute.datamunger.reader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
		Map<String, Integer> header = IntStream.range(0, headerList.size())
											   .boxed()
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
			
			aggregates = this.calclulateAggregates(result, queryParameter.getHeader(), queryParameter.getAggregateFunctions());
			
			if (null != aggregates) {
				dataSet.setAggregateFunctions(aggregates);	
				List<String> aggregateResults=new ArrayList<String>();
				for(AggregateFunction aggregate:aggregates) {
					aggregateResults.add(aggregate.getFunction()+"("+aggregate.getField()+") :"+aggregate.getResult());
				}
				List<List<String>> aggregateResultsList=new ArrayList<List<String>>();
				aggregateResultsList.add(aggregateResults);
				dataSet.setResult(aggregateResultsList);
			}
		}
		return dataSet;
	}

	/**
	 * @param queryParameter
	 * @param groupedDataSetMap
	 * @param dataSetMap
	 * @throws NumberFormatException
	 */
	private List<AggregateFunction> calclulateAggregates(final List<List<String>> result, Map<String, Integer> header,
			List<AggregateFunction> aggregates) throws NumberFormatException {
		if (null != aggregates && null != result) {
			for (final AggregateFunction aggregateFunction : aggregates) {
				final StringBuilder stringBuilder = new StringBuilder(aggregateFunction.getFunction());
				stringBuilder.append("(" + aggregateFunction.getField() + ")");
				aggregateFunction.setAggregateFieldIndex(header.get(aggregateFunction.getField()));
				switch (aggregateFunction.getFunction()) {
				case "count":
					final List<String> list = getCountAggregate(result, aggregateFunction);
					aggregateFunction.setResult(list.size());
					break;
				case "min":
					final int min = getMinAggregate(result, aggregateFunction);
					aggregateFunction.setResult(min);
					break;
				case "max":
					final int max = getMaxAggregate(result, aggregateFunction);
					aggregateFunction.setResult(max);
					break;
				case "sum":
					int sum = getSumAggregate(result, aggregateFunction);
					aggregateFunction.setResult(sum);
					break;
				case "avg":
					sum = (int) getAvgAggregate(result, aggregateFunction);
					aggregateFunction.setResult(sum / result.size());
					break;
				default:
					break;
				}
			}
		}
		return aggregates;
	}
	/**
	 * @param dataSetMap
	 * @param aggregateFunction
	 * @return
	 */
	private List<String> getCountAggregate(final List<List<String>> result, final AggregateFunction aggregateFunction) {
		final List<String> list = new ArrayList<String>();
		for (List<String> record : result) {
			final String value = record.get(aggregateFunction.getAggregateFieldIndex());
			if (null != value && !value.isEmpty()) {
				list.add(value);
			}
		}
		return list;
	}

	/**
	 * Get Average Value.
	 * 
	 * @param dataSetMap
	 * @param aggregateFunction
	 * @return
	 * @throws NumberFormatException
	 */
	private double getAvgAggregate(final List<List<String>> result, final AggregateFunction aggregateFunction)
			throws NumberFormatException {
		double sum;
		sum = 0;
		for (List<String> record : result) {
			final String fieldValue = record.get(aggregateFunction.getAggregateFieldIndex());
			if (null != fieldValue) {
				sum += Integer.parseInt(fieldValue);
			}
		}
		return sum;
	}

	/**
	 * Get Sum Value.
	 * 
	 * @param dataSetMap
	 * @param aggregateFunction
	 * @return
	 * @throws NumberFormatException
	 */
	private int getSumAggregate(final List<List<String>> result, final AggregateFunction aggregateFunction)
			throws NumberFormatException {
		int sum = 0;
		for (List<String> record : result) {
			final String fieldValue = record.get(aggregateFunction.getAggregateFieldIndex());
			if (null != fieldValue) {
				sum += Integer.parseInt(fieldValue);
			}
		}
		return sum;
	}

	/**
	 * Get Max Value. 
	 * @param dataSetMap
	 * @param aggregateFunction
	 * @param max
	 * @return
	 * @throws NumberFormatException
	 */
	private int getMaxAggregate(final List<List<String>> result, final AggregateFunction aggregateFunction)
			throws NumberFormatException {
		int max = 0;
		for (List<String> record : result) {
			final String fieldValue = record.get(aggregateFunction.getAggregateFieldIndex());
			if (null != fieldValue && max < Integer.parseInt(fieldValue)) {
				max = Integer.parseInt(fieldValue);
			}
		}
		return max;
	}

	/**
	 * Get Min Value.
	 * 
	 * @param dataSetMap
	 * @param aggregateFunction
	 * @return
	 * @throws NumberFormatException
	 */
	private int getMinAggregate(final List<List<String>> result, final AggregateFunction aggregateFunction)
			throws NumberFormatException {
		int min = Integer.MAX_VALUE;
		for (List<String> record : result) {
			final String fieldValue = record.get(aggregateFunction.getAggregateFieldIndex());
			if (null != fieldValue && min > Integer.parseInt(fieldValue)) {
				min = Integer.parseInt(fieldValue);
			}
		}
		return min;
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
