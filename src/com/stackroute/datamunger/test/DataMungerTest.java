package com.stackroute.datamunger.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Restriction;

public class DataMungerTest {

	private static Query query;

	private static QueryParser queryParser;
	private static QueryParameter queryParameter;
	private String queryString;

	@BeforeClass
	public static void init() {
		query = new Query();
		queryParser = new QueryParser();
	}

	@Test
	public void getFileNameTestCase() {
		queryString = "select * from data/ipl.csv";
		queryParameter = queryParser.parseQuery(queryString);
		assertEquals("data/ipl.csv", queryParameter.getFile());
		display(queryString, queryParameter);
	}

	@Test
	public void getFieldsTestCase() {
		queryString = "select team1, team2, winner, city from data/ipl.csv";
		queryParameter = queryParser.parseQuery(queryString);
		List<String> expectedFields = new ArrayList<>();
		expectedFields.add("team1");
		expectedFields.add("team2");
		expectedFields.add("winner");
		expectedFields.add("city");
		assertArrayEquals(expectedFields.toArray(), queryParameter.getFields().toArray());
		display(queryString, queryParameter);
	}

	@Test
	public void getFieldsAndRestrictionsTestCase() {
		queryString = "select team1, team2, winner, city from data/ipl.csv where win_by_runs>30 and city = 'Bangalore'";
		queryParameter = queryParser.parseQuery(queryString);
		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(restrictions);
		;

		display(queryString, queryParameter);
	}

	@Test
	public void getRestrictionsAndAggregateFunctionsTestCase() {
		queryString = "select count(city),sum(win_by_runs),min(season),max(win_by_wickets) from data/ipl.csv where season > 2014 and city ='Bangalore'";
		queryParameter = queryParser.parseQuery(queryString);
		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(restrictions);
		;

		display(queryString, queryParameter);
	}

	@Test
	public void getGroupByAndOrderByTestCase() {
		queryString = "select city,winner,team1,team2 from data/ipl.csv where season > 2016 and city='Bangalore' group by winner order by city";
		queryParameter = queryParser.parseQuery(queryString);
		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(restrictions);
		;

		display(queryString, queryParameter);
	}

	@Test
	public void getGroupByTestCase() {
		queryString = "select winner,count(*) from data/ipl.csv where season > 2016 group by winner";
		queryParameter = queryParser.parseQuery(queryString);
		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(restrictions);
		;

		display(queryString, queryParameter);
	}

	@Test
	public void getOrderByAndWhereConditionTestCase() {
		queryString = "select city,winner,team1,team2 from data/ipl.csv where season > 2016 and city='Bangalore' order by city";
		queryParameter = queryParser.parseQuery(queryString);
		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(restrictions);
		;

		display(queryString, queryParameter);
	}

	@Test
	public void getOrderByTestCase() {
		queryString = "select city,winner,team1,team2 from data/ipl.csv where city='Bangalore' order by winner";
		queryParameter = queryParser.parseQuery(queryString);
		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(restrictions);
		;

		display(queryString, queryParameter);
	}

	@Test
	public void getOrderByWihtoutWhereTestCase() {
		queryString = "select city,winner,team1,team2,player_of_match from data/ipl.csv order by city";
		queryParameter = queryParser.parseQuery(queryString);
		List<String> orderByFields = queryParameter.getOrderByFields();
		assertNotNull(orderByFields);
		display(queryString, queryParameter);
	}

	@Test
	public void getGroupByWithoutWhereTestCase() {
		queryString = "select winner,count(*) from data/ipl.csv group by winner";
		queryParameter = queryParser.parseQuery(queryString);
		List<String> groupByFields = queryParameter.getGroupByFields();
		assertNotNull(groupByFields);

		display(queryString, queryParameter);
	}

	private void display(String queryString, QueryParameter queryParameter) {
		System.out.println("\nQuery : " + queryString);
		System.out.println("--------------------------------------------------");
		System.out.println("Base Query:" + queryParameter.getBaseQuery());
		System.out.println("File:" + queryParameter.getFile());
		System.out.println("Query Type:" + queryParameter.getQUERY_TYPE());
		List<String> fields = queryParameter.getFields();
		System.out.println("Selected field(s):");
		if (fields == null || fields.isEmpty()) {
			System.out.println("*");
		} else {
			for (String field : fields) {
				System.out.println("\t" + field);
			}
		}

		List<Restriction> restrictions = queryParameter.getRestrictions();

		if (restrictions != null && !restrictions.isEmpty()) {
			System.out.println("Where Conditions : ");
			int conditionCount = 1;
			for (Restriction restriction : restrictions) {
				System.out.println("\tCondition : " + conditionCount++);
				System.out.println("\t\tName : " + restriction.getPropertyName());
				System.out.println("\t\tCondition : " + restriction.getCondition());
				System.out.println("\t\tValue : " + restriction.getPropertyValue());
			}
		}
		List<AggregateFunction> aggregateFunctions = queryParameter.getAggregateFunctions();
		if (aggregateFunctions != null && !aggregateFunctions.isEmpty()) {

			System.out.println("Aggregate Functions : ");
			int funtionCount = 1;
			for (AggregateFunction aggregateFunction : aggregateFunctions) {
				System.out.println("\t Aggregate Function : " + funtionCount++);
				System.out.println("\t\t function : " + aggregateFunction.getFunction());
				System.out.println("\t\t  field : " + aggregateFunction.getField());
			}

		}

		List<String> orderByFields = queryParameter.getOrderByFields();
		if (orderByFields != null && !orderByFields.isEmpty()) {

			System.out.println(" Order by fields : ");
			for (String orderByField : orderByFields) {
				System.out.println("\t " + orderByField);

			}

		}

		List<String> groupByFields = queryParameter.getGroupByFields();
		if (groupByFields != null && !groupByFields.isEmpty()) {

			System.out.println(" Group by fields : ");
			for (String groupByField : groupByFields) {
				System.out.println("\t " + groupByField);

			}

		}
	}

	@Test
	public void selectAllWithoutWhereTestCase() {

		DataSet dataSet = query.executeQuery("select * from data/ipl.csv");
		assertNotNull(dataSet);
		display("selectAllWithoutWhereTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereTestCase() {

		DataSet dataSet = query.executeQuery("select city,winner,team1,team2 from data/ipl.csv");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereTestCase", dataSet);

	}

	@Test
	public void withWhereGreaterThanTestCase() {

		DataSet dataSet = query.executeQuery(
				"select season,city,winner,team1,team2,player_of_match from data/ipl.csv where season > 2015");
		assertNotNull(dataSet);
		display("withWhereGreaterThanTestCase", dataSet);

	}

	@Test
	public void withWhereLessThanTestCase() {

		DataSet dataSet = query
				.executeQuery("select city,winner,team1,team2,player_of_match from data/ipl.csv where season < 2015");
		assertNotNull(dataSet);
		display("withWhereLessThanTestCase", dataSet);

	}

	@Test
	public void withWhereLessThanOrEqualToTestCase() {

		DataSet dataSet = query.executeQuery(
				"select season,city,winner,team1,team2,player_of_match from data/ipl.csv where season <= 2015");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereGreaterThanOrEqualToTestCase() {

		DataSet dataSet = query
				.executeQuery("select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2015");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereNotEqualToTestCase() {

		DataSet dataSet = query.executeQuery(
				"select city,team1,team2,winner,toss_decision from data/ipl.csv where toss_decision != bat");
		assertNotNull(dataSet);
		display("withWhereNotEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereEqualAndNotEqualTestCase() {

		DataSet dataSet = query.executeQuery(
				"select season,city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2013 and season <= 2015");
		assertNotNull(dataSet);
		display("withWhereEqualAndNotEqualTestCase", dataSet);

	}

	@Test
	public void withWhereTwoConditionsEqualOrNotEqualTestCase() {

		DataSet dataSet = query.executeQuery(
				"select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2013 and toss_decision != bat");
		assertNotNull(dataSet);
		display("withWhereTwoConditionsEqualOrNotEqualTestCase", dataSet);

	}

	@Test
	public void withWhereThreeConditionsEqualOrNotEqualTestCase() {

		DataSet dataSet = query.executeQuery(
				"select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2015 or toss_decision != bat and city = bangalore");
		assertNotNull(dataSet);
		display("withWhereTwoConditionsEqualOrNotEqualTestCase", dataSet);

	}

	@Test
	public void withWhereThreeConditionsOrderByTestCase() {

		DataSet dataSet = query.executeQuery(
				"select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2013 or toss_decision != bat and city = Bangalore order by winner");
		assertNotNull(dataSet);
		display("withWhereThreeConditionsOrderByTestCase", dataSet);

	}

	@Test
	public void withWhereThreeConditionsGroupByOrderByTestCase() {

		DataSet dataSet = query.executeQuery(
				"select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2013 or toss_decision != bat and city = Bangalore group by team1");
		assertNotNull(dataSet);
		displayGroupByResult(queryString, dataSet.getGroupByResult());

	}

	@Test
	public void withOrderByTestCase() {

		DataSet dataSet = query
				.executeQuery("select city,winner,team1,team2,player_of_match from data/ipl.csv order by city");
		assertNotNull(dataSet);
		display("withOrderByTestCase", dataSet);

	}

	@Test
	public void selectCountColumnsWithoutWhereTestCase() {

		DataSet dataSet = query.executeQuery("select count(city) from data/ipl.csv");
		assertNotNull(dataSet);
		display("selectCountColumnsWithoutWhereTestCase", dataSet);
	}

	@Test
	public void selectSumColumnsWithoutWhereTestCase() {

		DataSet dataSet = query.executeQuery("select sum(win_by_runs) from data/ipl.csv");
		assertNotNull(dataSet);
		display("selectSumColumnsWithoutWhereTestCase", dataSet);

	}

	@Test
	public void selectCountColumnsWithoutWhereTestCase2() {

		DataSet dataSet = query.executeQuery(
				"select count(city),sum(win_by_runs),min(win_by_runs),max(win_by_runs),avg(win_by_runs) from data/ipl.csv");
		assertNotNull(dataSet);
		display("selectCountColumnsWithoutWhereTestCase2", dataSet);
	}

	@Test
	public void selectColumnsWithoutWhereWithGroupByCountTestCase() {

		DataSet dataSet = query.executeQuery("select city,count(*) from data/ipl.csv group by city");
		assertNotNull(dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());
	}

	@Test
	public void selectColumnsWithoutWhereWithGroupBySumTestCase() {

		DataSet dataSet = query.executeQuery("select city,sum(season) from data/ipl.csv group by city");
		assertNotNull(dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());

	}

	@Test
	public void selectColumnsWithoutWhereWithGroupByMinTestCase() {

		DataSet dataSet = query.executeQuery("select city,min(season) from data/ipl.csv group by city");
		assertNotNull(dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());

	}

	@Test
	public void selectColumnsWithoutWhereWithGroupByMaxTestCase() {

		DataSet dataSet = query.executeQuery("select city,max(win_by_wickets) from data/ipl.csv group by city");
		assertNotNull(dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());

	}

	@Test
	public void selectColumnsWithoutWhereWithGroupByAvgTestCase() {

		DataSet dataSet = query.executeQuery("select city,avg(win_by_wickets) from data/ipl.csv group by city");
		assertNotNull(dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());

	}

	private void display(String testCaseName, DataSet records) {
		System.out.println("\nTest Case Name : " + testCaseName);
		records.getResult().forEach(System.out::println);

	}

	private void displayGroupByAggregateResult(String queryString, Map<String, DoubleSummaryStatistics> groupByResult) {
		System.out.println("\nGiven Query : " + queryString);

		groupByResult.entrySet().forEach(System.out::println);

		// TODO Auto-generated method stub

	}

	private void displayGroupByResult(String queryString, Map<String, List<List<String>>> groupByResult) {
		System.out.println("\nGiven Query : " + queryString);
		groupByResult.entrySet().forEach(System.out::println);

	}

}
