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
	 * Regex for white space.
	 */
	private static final String WHITE_SPACE = "\\s+";

	/**
	 * Regex for Non word characters.
	 */
	private static final String NON_WORD_CHAR = "\\w+";

	/**
	 * Regex for logical operators.
	 */
	private static final String LOGICAL_OPERATORS = "\\s+and\\s+|\\s+or\\s+|\\s+not\\s+";


/**
 *This method used to parse the query. (See the comment above the class)
 * Note:  Better to write separate private method to extract each part of the query
 * Ex: private List<String> getFields(String baseQuery) 
 * Ex: private List<String> getLogicalOperators()
 * etc.,
 * This is the way you can achieve modularity and readability.
 **/
	public QueryParameter parseQuery(String queryString) {
		if (null != queryString && !queryString.isEmpty()) {

			/*
			 * extract the name of the file from the query. File name can be found after the
			 * "from" clause.
			 */
			final String file = this.getFile(queryString);
			queryParameter.setFile(file);

			// extract baseQuery
			final String baseQuery = this.getBaseQuery(queryString);
			if (null != baseQuery) {
				queryParameter.setBaseQuery(baseQuery);
			}
			// extract fields
			final List<String> fields = this.getFields(queryString);
			if (null != fields) {
				queryParameter.setFields(fields);
			}

			// extract the order by fields from the query string.
			final List<String> orderByFields = this.getOrderByFields(queryString);
			if (null != orderByFields) {
				queryParameter.setOrderByFields(orderByFields);
			}

			// extract the group by fields from the query string.
			final List<String> groupByFields = this.getGroupByFields(queryString);
			if (null != groupByFields) {
				queryParameter.setGroupByFields(groupByFields);
			}

			// Extracting Conditions.
			final List<Restriction> restrictionList = this.getConditions(queryString);
			if (null != restrictionList) {
				queryParameter.setRestrictions(restrictionList);
			}

			// Extracting logical Operators
			final List<String> operatorsList = this.getLogicalOperators(queryString);
			if (null != operatorsList) {
				queryParameter.setLogicalOperators(operatorsList);
			}

			// Extracting Aggregate functions.
			final List<AggregateFunction> aggregateList = this.getAggregateFunctions(queryString);
			if (null != aggregateList) {
				queryParameter.setAggregateFunctions(aggregateList);
			}

			this.determineQueryType(queryParameter);
		}
		return queryParameter;
	}


	/**
	 * @param queryParameter
	 */
	private void determineQueryType(final QueryParameter queryParameter) {
		if (null != queryParameter.getRestrictions() && !queryParameter.getRestrictions().isEmpty()) {
			queryParameter.setQUERY_TYPE("WHERE_CLAUSE_QUERY");
		}

		if (null != queryParameter.getAggregateFunctions() && !queryParameter.getAggregateFunctions().isEmpty()) {
			queryParameter.setQUERY_TYPE("AGGREGATE_QUERY");
		}

		if (null != queryParameter.getOrderByFields() && !queryParameter.getOrderByFields().isEmpty()) {
			queryParameter.setQUERY_TYPE("ORDER_BY_QUERY");
		}

		if (null != queryParameter.getGroupByFields() && !queryParameter.getGroupByFields().isEmpty()) {
			queryParameter.setQUERY_TYPE("GOUP_BY_QUERY");
		}

		if (null != queryParameter.getGroupByFields() && !queryParameter.getGroupByFields().isEmpty()
				&& null != queryParameter.getAggregateFunctions()
				&& !queryParameter.getAggregateFunctions().isEmpty()) {
			queryParameter.setQUERY_TYPE("GOUP_BY_AGGREGATE_QUERY");
		}
		
		if (null != queryParameter.getGroupByFields() && !queryParameter.getGroupByFields().isEmpty()
				&& null != queryParameter.getOrderByFields() && !queryParameter.getOrderByFields().isEmpty()) {
			queryParameter.setQUERY_TYPE("GOUP_BY_ORDER_BY_QUERY");
		}
	}

	/**
	 * extract the name of the file from the query. File name can be found after the
	 * "from" clause.
	 */
	public String getFile(final String queryString) {
		String returnString = null;
		final String[] fromSplit = queryString.split(" from ");
		if (null != fromSplit && fromSplit.length > 1) {
			final String tempStr = fromSplit[1];
			final String[] tempStrArray = tempStr.split(WHITE_SPACE);
			returnString = tempStrArray[0];
		}
		return returnString;
	}

	/**
	 * extract the order by fields from the query string. Please note that we will
	 * need to extract the field(s) after "order by" clause in the query, if at all
	 * the order by clause exists. For eg: select city,winner,team1,team2 from
	 * data/ipl.csv order by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one order by fields.
	 */
	public List<String> getOrderByFields(final String queryString) {
		final List<String> list = new ArrayList<String>();
		final String[] strArray = queryString.split(WHITE_SPACE);
		boolean orderByFound = false;
		for (int i = 1; i < strArray.length; i++) {
			if (strArray[i].equalsIgnoreCase("Order") && strArray[i + 1].equalsIgnoreCase("by")) {
				orderByFound = true;
				i++;
				continue;
			}
			if (orderByFound) {
					String[] orderByFields = strArray[i].split(",");
					if (null != orderByFields) {
						for(String orderByField : orderByFields) {
							list.add(orderByField.replaceAll("order by", "").trim());
						}
					}
			}
		}
		return list;
	}

	/**
	 * extract the group by fields from the query string. Please note that we will
	 * need to extract the field(s) after "group by" clause in the query, if at all
	 * the group by clause exists. For eg: select city,max(win_by_runs) from
	 * data/ipl.csv group by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one group by fields.
	 */
	public List<String> getGroupByFields(final String queryString) {
		final String[] strArray = queryString.trim().split(WHITE_SPACE);
		boolean groupByFound = false;
		for (int i = 1; i < strArray.length; i++) {
			if (strArray[i].equalsIgnoreCase("Group") && strArray[i + 1].equalsIgnoreCase("by")) {
				groupByFound = true;
				i++;
				continue;
			}
			List<String> groupByList = new ArrayList<String>();
			if (groupByFound) {
				String[] groupByFields = strArray[i].split(",");
				if (null != groupByFields) {
					for(String grpByField : groupByFields) {
						groupByList.add(grpByField.replaceAll("group by", "").trim());
					}
				}
				return groupByList;
			}
		}
		return null;
	}

	/**
	 * extract the selected fields from the query string. Please note that we will
	 * need to extract the field(s) after "select" clause followed by a space from
	 * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
	 * query mentioned above, we need to extract "city" and "win_by_runs". Please
	 * note that we might have a field containing name "from_date" or "from_hrs".
	 * Hence, consider this while parsing.
	 */
	public List<String> getFields(final String queryString) {
		final List<String> fields = new ArrayList<String>();
		final String[] selectSplit = queryString.trim().split("select ");
		if (null != selectSplit && selectSplit.length > 1) {
			final String[] fromSplit = selectSplit[1].split(" from ");
			final String[] commaSplit = fromSplit[0].split(",");
			for (int i = 0; i < commaSplit.length; i++) {
				if (!commaSplit[i].contains("(") && !commaSplit[i].contains(")")) {
					fields.add(commaSplit[i].trim());
				}
			}
		}
		return fields;
	}

	/**
	 * extract the conditions from the query string(if exists). for each condition,
	 * we need to capture the following: 1. Name of field 2. condition 3. value
	 * 
	 */
	public List<Restriction> getConditions(final String queryString) {

		final String conditionQuery = this.getConditionsPartQuery(queryString);

		if (null == conditionQuery) {
			return null;
		}

		final String[] strArray = conditionQuery.split(LOGICAL_OPERATORS);
		final List<Restriction> list = new ArrayList<Restriction>();
		for (String str : strArray) {
			str = str.replaceAll(WHITE_SPACE, "");
			str = str.replaceAll("'", "");
			final String[] nonWordCharArray = str.split(NON_WORD_CHAR);
			if (null != nonWordCharArray && nonWordCharArray.length > 1) {
				final String nonWordChar = nonWordCharArray[1];
				final String alphaArray[] = str.split(nonWordChar);
				if (null != alphaArray && alphaArray.length > 1) {
					final Restriction restriction = new Restriction(alphaArray[0], alphaArray[1], nonWordChar);
					// System.out.println(restriction.toString());
					list.add(restriction);
				}
			}
		}
		return list;
	}

	/**
	 * extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 */
	public List<String> getLogicalOperators(final String queryString) {
		final List<String> conditions = new ArrayList<String>();
		final String[] strArray = queryString.trim().split(WHITE_SPACE);
		boolean whereFound = false;
		for (int i = 1; i < strArray.length - 1; i++) {
			if (strArray[i].equalsIgnoreCase("WHERE")) {
				whereFound = true;
			}
			if (whereFound && ("and".equalsIgnoreCase(strArray[i]) || "or".equalsIgnoreCase(strArray[i])
					|| "not".equalsIgnoreCase(strArray[i]))) {
				conditions.add(strArray[i]);
			}
		}
		return conditions.isEmpty() ? null : conditions;
	}

	/**
	 * extract the aggregate functions from the query. The presence of the aggregate
	 * functions can determined if we have either "min" or "max" or "sum" or "count"
	 * or "avg" followed by opening braces"(" after "select" clause in the query
	 * string. in case it is present, then we will have to extract the same. For
	 * each aggregate functions, we need to know the following: 1. type of aggregate
	 * function(min/max/count/sum/avg) 2. field on which the aggregate function is
	 * being applied
	 * 
	 */
	public List<AggregateFunction> getAggregateFunctions(final String queryString) {
		final List<AggregateFunction> aggregateList = new ArrayList<AggregateFunction>();
		final String[] strArray = queryString.trim().split("select ");
		if (null != strArray && strArray.length > 1) {
			final String beforeFromQuery = strArray[1].split(" from ")[0];
			if (beforeFromQuery.contains("sum(") || beforeFromQuery.contains("count(")
					|| beforeFromQuery.contains("max(") || beforeFromQuery.contains("min(")
					|| beforeFromQuery.contains("avg(")) {

				final String[] aggregateArr = beforeFromQuery.split(",");
				if (null != aggregateArr) {
					for (int j = 0; j < aggregateArr.length; j++) {
						final String[] splittedStr = aggregateArr[j].split("\\(");
						if (null != splittedStr && splittedStr.length > 1) {
							final AggregateFunction aggregateFunction = new AggregateFunction();
							aggregateFunction.setField(splittedStr[1].substring(0, splittedStr[1].length() - 1));
							aggregateFunction.setFunction(splittedStr[0]);
							aggregateList.add(aggregateFunction);
						}
					}
				}
			}
		}

		return aggregateList.isEmpty() ? null : aggregateList;
	}

	/**
	 * Method to get condition part query.
	 * 
	 * @param queryString
	 * @return
	 */
	public String getConditionsPartQuery(final String queryString) {
		if (queryString.contains(" where ")) {
			final String afterWhere = queryString.split("\\s+where\\s+")[1];
			if (afterWhere.contains(" order by ")) {
				return afterWhere.split("\\s+order by\\s+")[0];
			}
			if (afterWhere.contains(" group by ")) {
				return afterWhere.split("\\s+group by\\s+")[0];
			}
			return afterWhere;
		}
		return null;
	}

	/**
	 * Method to get Base part query.
	 * 
	 * @param queryString
	 * @return
	 */
	public String getBaseQuery(final String queryString) {
		String baseQuery = queryString.split("where")[0];
		if (baseQuery.contains(" order by ")) {
			return baseQuery = queryString.split("order by")[0].trim();
		}
		if (baseQuery.contains(" group by ")) {
			return baseQuery = queryString.split("group by")[0].trim();
		}
		return baseQuery.trim();
	}

}
