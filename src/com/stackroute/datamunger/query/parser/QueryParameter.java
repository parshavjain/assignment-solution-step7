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
//Declare the query parameters listed above and generate getter/setter methods.
//No other logic should present in this class.
//This is simple POJO Class ( A Class with properties and getter/setter methods.)
	

	//this class contains the parameters and accessor/mutator methods of QueryParameter
			private String queryString;
			private List<Restriction> restrictions;
			private List<String> logicalOperators;
			private List<AggregateFunction> aggregateFunctions;
			private String file;
			// query without where condition
			private String baseQuery;
			// Selected fields. If it is null -> * ( i.e., select all fields)
			private List<String> fields;
			private List<String> groupByFields;
			private List<String> orderByFields;
			// Query type may be simple, group by, order by, aggregate
			private String QUERY_TYPE = "SIMPLE_QUERY";
			// place holder for header
			private Map<String, Integer> header;
			
			//place holder for selected field index
			private List<Integer> selectedFieldIndexes;
			
			
			public List<Integer> getSelectedFieldIndexes() {
				return selectedFieldIndexes;
			}
			public void setSelectedFieldIndexes(List<Integer> selectedFieldIndexes) {
				this.selectedFieldIndexes = selectedFieldIndexes;
			}
			public Map<String, Integer> getHeader() {
				return header;
			}
			public void setHeader(Map<String, Integer> header) {
				this.header = header;
			}
			public String getQUERY_TYPE() {
				return QUERY_TYPE;
			}
			public void setQUERY_TYPE(String qUERY_TYPE) {
				QUERY_TYPE = qUERY_TYPE;
			}
			public String getFile() {
				return file;
			}
			public void setFile(String file) {
				this.file = file;
			}
			public String getQueryString() {
				return queryString;
			}
			public void setQueryString(String queryString) {
				this.queryString = queryString;
			}
			public List<Restriction> getRestrictions() {
				return restrictions;
			}
			public void setRestrictions(List<Restriction> restrictions) {
				this.restrictions = restrictions;
			}
			public List<String> getLogicalOperators() {
				return logicalOperators;
			}
			public void setLogicalOperators(List<String> logicalOperators) {
				this.logicalOperators = logicalOperators;
			}
			public String getBaseQuery() {
				return baseQuery;
			}
			public void setBaseQuery(String baseQuery) {
				this.baseQuery = baseQuery;
			}
			public List<String> getFields() {
				return fields;
			}
			public void setFields(List<String> fields) {
				this.fields = fields;
			}
			public List<AggregateFunction> getAggregateFunctions() {
				return aggregateFunctions;
			}
			public void setAggregateFunctions(List<AggregateFunction> aggregateFunctions) {
				this.aggregateFunctions = aggregateFunctions;
			}
			public List<String> getGroupByFields() {
				return groupByFields;
			}
			public void setGroupByFields(List<String> groupByFields) {
				this.groupByFields = groupByFields;
			}
			public List<String> getOrderByFields() {
				return orderByFields;
			}
			public void setOrderByFields(List<String> orderByFields) {
				this.orderByFields = orderByFields;
			}

		

			
}
