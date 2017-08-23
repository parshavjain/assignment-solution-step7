package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.List;
/**
 *This class is used to the parse the query. 
 * It means split the query into various tokens and extract file name, selected fields, where condition
 * group by field, aggregate field, order by field
 * Also decide what type of query it is and set to QueryParameter instance.
 * Once the QueryParameter instance is created, this object can be used to execute the query.
 * 
 **/
public class QueryParser {

	private QueryParameter queryParameter = new QueryParameter();

/**
 *This method used to parse the query. (See the comment above the class)
 * Note:  Better to write separate private method to extract each part of the query
 * Ex: private List<String> getFields(String baseQuery) 
 * Ex: private List<String> getLogicalOperators()
 * etc.,
 * This is the way you can achieve modularity and readability.
 **/
	public QueryParameter parseQuery(String queryString) {
		
		return null;
	}
	
}
