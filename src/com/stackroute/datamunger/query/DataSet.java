package com.stackroute.datamunger.query;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.parser.AggregateFunction;

//This class will be acting as the ResultSet containing multiple rows.
//The type (data type) of the result will different for different types of query.
//Like ,simple query, aggregate, group by etc.,

//You have to decide what should be data type for each type of query.
//You can use List<> OR Map<> based on requirement.

//Example:
//Ex:1 - For Simple Query(without aggregate, group by)
//  private List<List<String>> result;

//Ex:2 - For Group by
// 	private Map<String, List<List<String>>> groupByResult;

//Note:  There are few built in classes in java 8. We have to use them
//like DoubleSummaryStatistics
public class DataSet {
	
	private List<List<String>> result;

	private List<AggregateFunction> aggregateFunctions;

	private Map<String, List<List<String>>> groupByResult;
	
	private Map<String, DoubleSummaryStatistics> groupByAggregateResult;

}
