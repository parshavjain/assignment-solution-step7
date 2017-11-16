package com.stackroute.datamunger.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

//This class is like utility class.
//We can define common methods which can be used acrross the project.
//Like filterFields, isRequiredRecord etc.,
//You can write private methods to do subtask of the above methods.
public class Filter {

	/**
	 * This method take the record and remove the fields from the record. The record
	 * consist of all the fields, you have to keep what use selected in the query.
	 */
	public List<String> filterFields(QueryParameter queryParameter, List<String> record) {
		List<String> finalFields = new ArrayList<String>();
		if (null != queryParameter && null != queryParameter.getFields() && null != queryParameter.getHeader()) {
			if (null != queryParameter.getAggregateFunctions()) {
				for (AggregateFunction aggregateFunction : queryParameter.getAggregateFunctions()) {
					queryParameter.getFields().add(aggregateFunction.getField());
				}
			}
			for (String field : queryParameter.getFields()) {
				if (field.equals("*")) {
					finalFields.addAll(record);
					break;
				}
				int index = queryParameter.getHeader().get(field);
				if (null != record && index < record.size()) {
					finalFields.add(record.get(index));
				}
			}
		}
		return finalFields;
	}

	/**
	 * This method takes record as input and return true if the record is required
	 * based on 'where' condition given in the query. Multiple conditions may be
	 * exist in "where" part of the query.
	 **/
	public boolean isRequiredRecord(QueryParameter queryParameter, List<String> record) {
		boolean finalResult = true;
		if (null != queryParameter && null != queryParameter.getRestrictions() && null != queryParameter.getHeader()) {
			finalResult = false;
			int index = 0;
			if (null != queryParameter.getLogicalOperators()) {
				index = queryParameter.getLogicalOperators().size() - 1;
			}
			String logicalOp = "or";
			for (int i = queryParameter.getRestrictions().size() - 1; i >= 0; i--) {
				List<String> list = new ArrayList<String>();
				int recordIndex = queryParameter.getHeader().get(queryParameter.getRestrictions().get(i).getPropertyName());
				list.add(recordIndex < record.size() ? record.get(recordIndex) : "");
				final boolean result = evaluateRelationalExpression(list, queryParameter.getRestrictions().get(i));

				/*
				 * check for multiple conditions in where clause for eg: where salary>20000 and
				 * city=Bangalore for eg: where salary>20000 or city=Bangalore and dept!=Sales
				 */
				if ("and".equals(logicalOp)) {
					finalResult = finalResult && result;
				} else if ("or".equals(logicalOp)) {
					finalResult = finalResult || result;
				}
				if (null != queryParameter.getLogicalOperators() && !queryParameter.getLogicalOperators().isEmpty()
						&& index >= 0) {
					logicalOp = queryParameter.getLogicalOperators().get(index);
					index--;
				}
			}
		}
		return finalResult;
	}

	/**
	 * This method return true if the record is required 'where' condition If not it
	 * returns false.
	 **/
	private boolean evaluateRelationalExpression(List<String> record, Restriction restriction) {
		boolean result = false;
		outer:for (String value : record) {
			switch (restriction.getCondition()) {
			case "=":
				result = this.equalsTo(restriction.getPropertyValue(), value);
				break outer;
			case "!=":
				result = !this.equalsTo(restriction.getPropertyValue(), value);
				break outer;
			case ">":
				result = this.greaterThan(restriction.getPropertyValue(), value);
				break outer;
			case "<":
				result = this.lessThan(restriction.getPropertyValue(), value);
				break outer;
			case "<=":
				result = this.lessThan(restriction.getPropertyValue(), value)
						|| this.equalsTo(restriction.getPropertyValue(), value);
				break outer;
			case ">=":
				result = this.equalsTo(restriction.getPropertyValue(), value)
						|| this.greaterThan(restriction.getPropertyValue(), value);
				break;
			default:
				break;
			}
		}
		return result;
	}

	// method containing implementation of equalTo operator
	private boolean equalsTo(final String value1, final String value2) {
		boolean flag = false;
		if (null != value1 && null != value2) {
			flag = value1.equalsIgnoreCase(value2);
		}
		return flag;
	}

	// method containing implementation of greaterThan operator
	private boolean greaterThan(final String restrictionVal, final String dataVal) {
		boolean flag = false;
		if (null != restrictionVal && null != dataVal) {
			try {
				final int restriction = Integer.parseInt(restrictionVal);
				final int data = Integer.parseInt(dataVal);
				flag = data > restriction;
			} catch (NumberFormatException e) {
				flag = false;
			}
		}
		return flag;
	}

	// method containing implementation of lessThan operator
	private boolean lessThan(final String restrictionVal, final String dataVal) {
		boolean flag = false;
		if (null != restrictionVal && null != dataVal) {
			try {
				final int restriction = Integer.parseInt(restrictionVal);
				final int data = Integer.parseInt(dataVal);
				flag = data < restriction;
			} catch (NumberFormatException e) {
				flag = false;
			}
		}
		return flag;
	}
}
