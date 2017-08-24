package com.stackroute.datamunger.query.parser;

import java.util.List;
import java.util.Map;

/**
 *This is the custom class to hold all the query parameters
 * Need to declare private properties and generate getter/setter methods
 * private String queryString;  // the complete query string
 * private List<Restriction> restrictions;  // List of restrictions.  Restrictin is another custom class
 * private List<String> logicalOperators;   //List of logical operators.  In the query ther may be multiple conditions.  
 * private List<AggregateFunction> aggregateFunctions;  //// List of agrregate functions.  AggregateFunction is another custom class
 * private String file;                            //To store the name of file
 * private String baseQuery;                       //To store the base query
 * private List<String> fields;                    //To store the list of selected fields
 * private List<String> groupByFields;             //To store the group by fields.
 * private List<String> orderByFields;             //To store the order by fields
 * private String QUERY_TYPE = "SIMPLE_QUERY";     //To store the query type.  Query type may be simple, aggregate, group by etc.,
 * private Map<String, Integer> header;            //To store the header ( i.e., first row.).  We will store the header while reading the data from file.
 * 
 **/
public class QueryParameter {
//Better to write separate (private)function to get each query parameter.
//Set each parameter to QueryParameter instance.
			
}
