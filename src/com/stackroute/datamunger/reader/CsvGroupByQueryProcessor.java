package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
		
			// read the remaining records
				return null;
	}
	
	/**
	 * This method is used to calculate group by summary with fields.  It means along with group by, some other fields
	 * also present in given query.
	 **/

	private Map<String, List<List<String>>> calclulateGroupByWithFields(List<List<String>> result,
			QueryParameter queryParameter) {
		
		return null;
	}
/**
 * This method is use to get index/position of group by field
 **/
	private int getGroupByFieldIndex(List<String> selectedFields, String groupByField) {
		
		
		return 0;
	}

	/**
	 * This method is used to calculate group by  along with other aggregate functions like min, max, avg, sum, count.
	 * Used streaps and filters of java 8
	 * Used DoubleSummaryStatistics a java 8 built-in class to store aggregate functions
	 **/
	private Map<String, DoubleSummaryStatistics> calclulateGroupByWithAggregates(List<List<String>> result,
			QueryParameter queryParameter) {

		return null;
	}

}
