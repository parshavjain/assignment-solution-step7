package com.stackroute.datamunger.query;

import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.reader.CsvAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByQueryProcessor;
import com.stackroute.datamunger.reader.CsvOrderByQueryProcessor;
import com.stackroute.datamunger.reader.CsvQueryProcessor;
import com.stackroute.datamunger.reader.CsvWhereQueryProcessor;
import com.stackroute.datamunger.reader.QueryProcessingEngine;

/**
 * This class is used to execute the query with the help of different Query
 * Processors. Decide what type of Query processors you required. Hint: For each
 * different query type you need different Query Procesor. You can write any
 * private methods if required(to be used in this class only)
 **/
public class Query {

	public DataSet executeQuery(String queryString) {

		// Get the type of query from queryString
		// Based on the type of query, create the instance of the particular class
		// Note: We created one interface called QueryEngine
		// and implemented this in other concrete classes like CsvWhereQueryProcessor,
		// CsvQueryProcessor,CsvOrderByQueryProcessor,CsvAggregateQueryProcessor
		// CsvGroupByQueryProcessor.
		// Once you create the instance of one of the above class, we can call
		// executeQuery method which overridden in all thse classes.
		// This executeQuery method will return the result set.
		/* instantiate QueryParser class */
		final QueryParser queryParser = new QueryParser();

		/*
		 * call parseQuery() method of the class by passing the queryString which will
		 * return object of QueryParameter
		 */
		final QueryParameter queryParameter = queryParser.parseQuery(queryString);

		/*
		 * Check for Type of Query based on the QueryParameter object. In this
		 * assignment, we will process queries containing zero, one or multiple where
		 * conditions i.e. conditions, aggregate functions, order by, group by clause
		 */

		/*
		 * call the getResultSet() method of CsvQueryProcessor class by passing the
		 * QueryParameter Object to it. This method is supposed to return resultSet
		 * which is a HashMap
		 */
		QueryProcessingEngine engine = null;
		if (null != queryParameter && null != queryParameter.getQUERY_TYPE()) {
			switch (queryParameter.getQUERY_TYPE()) {
			case "WHERE_CLAUSE_QUERY":
				engine = new CsvWhereQueryProcessor();
				break;
			case "SIMPLE_QUERY":
				engine = new CsvQueryProcessor();
				break;
			case "GOUP_BY_AGGREGATE_QUERY":
				engine = new CsvGroupByQueryProcessor();
				break;
			case "AGGREGATE_QUERY":
				engine = new CsvAggregateQueryProcessor();
				break;
			case "GOUP_BY_QUERY":
				engine = new CsvGroupByQueryProcessor();
				break;
			case "ORDER_BY_QUERY":
				engine = new CsvOrderByQueryProcessor();
				break;
			case "GOUP_BY_ORDER_BY_QUERY":
				engine = new CsvGroupByQueryProcessor();
				break;
			default:
				engine = new CsvQueryProcessor();
				break;
			}
		}
		

		return null != engine ? engine.executeQuery(queryParameter) : null;
	}

}
