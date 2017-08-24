package com.stackroute.datamunger.query;


/**
 * This class is used to execute the query with the help of different Query Processors.
 * Decide what type of Query processors you required.
 * Hint:  For each different query type you need different Query Procesor.
 * You can write any private methods if required(to be used in this class only)
 **/
public class Query {

	
	public DataSet executeQuery(String queryString) {
		
	
	//Get the type of query from queryString
	//Based on the type of query,  create the instance of the particular class
	//Note:  We created one interface called QueryEngine 
	//and implemented this in other concrete classes like CsvWhereQueryProcessor, CsvQueryProcessor,CsvOrderByQueryProcessor,CsvAggregateQueryProcessor
	//CsvGroupByQueryProcessor.
	//Once you create the instance of one of the above class, we can call executeQuery method which overridden in all thse classes.
	//This executeQuery method will return the result set.
		
		return null;
	}
	


}
