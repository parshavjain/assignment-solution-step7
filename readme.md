## Seed code - Boilerplate for step 7 - Database Engine Assignment

### Problem Statement

In this assignment step 7, we will try to filter the data from CSV file with much more **complex queries using lambda expression predicate or streams**. Our query is expected to contain **aggregate functions, group by, order by, aggregate group by.**

As the Data type of particular column is required only when we sort / compare with relational operators/find aggregate functions, at that point only we can find what is the type of data. Even to compare whether the data is equal / not equal, hence we no need to find data type.                                                                                                                                                                                                                                                                                                                                
As the data in CSV file are the string, we can use equals() built-in method.  Only for comparing <. >, <=, >= we need to find the type of data. Whenever query contains this relational operator, we can convert the String data into the required data type. For Aggregate function - count - also we need not to find out the datatype.                                                                                                                                                           
For other aggregate functions like sum, min, max, total we need to find out data type.     
When the query consists of an aggregate function, then only we convert into the required data type.  Otherwise, it is not required.

### In this assignment, 
1. We will use Java 8 concepts like lamda expressions, predicates and streams.  We will work with array list.  
2. We will get all aggregate functions together even we require only one aggregate function.  This is because we are using Java 8 built in classes like 
   **DoubleSummaryStatistics, LongSummaryStatistics , IntSummaryStatistics**
3. We will not use DataTypeDefinition and RowTypeDefinition classes. 
    Actually the data type is not required for all type of queries. Like "selet * from ipl.csv" we no need to find out data type.
    So, whenever we required then only we will convert the data into specific type.
   
     **When we required the data type ?**

    a)We require Data type of particular column when we sort / compare with relational operators/find aggregate functions.  
    At that point only we can find what is the type of data.                                                                                                                                                                                                     
    
    b)Even to compare whether the data equal / not equal, we no need to find data type.                                                                                                                                                   
    As the data in csv file are string, we can use equals() built-in method.  Only for comparing <. >, <=, >= only we need to find the type of data. 
    Whenever query contains this relational operator, we can convert the String data into required data type.                                                                                                               
    
    c)For Aggregate function - count - also no need to find out data type.                                                                                                                                                           
    For other aggregate functions like sum, min, max, total only we need to find out data type.    
    When the query consist of aggregate function, then only we convert into required data type.  Otherwise it is not required.

4.  We will not implement JSON convertion as well(as of now).  The main intention of this step 7 is to work with java 8 concepts like lamda expressions, predicates and streams.

### Sample queires and output

For Example

1. Input from the User : **select city,avg(win_by_wickets) from data/ipl.csv group by city**
        
        Expected Output: 
        =DoubleSummaryStatistics{count=7, sum=23.000000, min=0.000000, average=3.285714, max=7.000000}
        Ahmedabad=DoubleSummaryStatistics{count=12, sum=38.000000, min=0.000000, average=3.166667, max=9.000000}
        Delhi=DoubleSummaryStatistics{count=53, sum=175.000000, min=0.000000, average=3.301887, max=10.000000}
        Sharjah=DoubleSummaryStatistics{count=6, sum=26.000000, min=0.000000, average=4.333333, max=8.000000}

2. Input from the User : **select city,max(win_by_wickets) from data/ipl.csv group by city**

        Expected Output:
        =DoubleSummaryStatistics{count=7, sum=23.000000, min=0.000000, average=3.285714, max=7.000000}
        Ahmedabad=DoubleSummaryStatistics{count=12, sum=38.000000, min=0.000000, average=3.166667, max=9.000000}
        Delhi=DoubleSummaryStatistics{count=53, sum=175.000000, min=0.000000, average=3.301887, max=10.000000}
        Sharjah=DoubleSummaryStatistics{count=6, sum=26.000000, min=0.000000, average=4.333333, max=8.000000}
        Rajkot=DoubleSummaryStatistics{count=5, sum=31.000000, min=0.000000, average=6.200000, max=10.000000}

3. Input from the User: **select city,sum(season) from data/ipl.csv group by city**

        Expected Output:
        =DoubleSummaryStatistics{count=7, sum=14098.000000, min=2014.000000, average=2014.000000, max=2014.000000}
        Ahmedabad=DoubleSummaryStatistics{count=12, sum=24156.000000, min=2010.000000, average=2013.000000, max=2015.000000}
        Delhi=DoubleSummaryStatistics{count=53, sum=106652.000000, min=2008.000000, average=2012.301887, max=2016.000000}
        Sharjah=DoubleSummaryStatistics{count=6, sum=12084.000000, min=2014.000000, average=2014.000000, max=2014.000000}
        Rajkot=DoubleSummaryStatistics{count=5, sum=10080.000000, min=2016.000000, average=2016.000000, max=2016.000000}

4. Input from the User: **select city,count(win_by_wickets) from data/ipl.csv group by city**
        
        Expected Output:
        =DoubleSummaryStatistics{count=7, sum=23.000000, min=0.000000, average=3.285714, max=7.000000}
        Ahmedabad=DoubleSummaryStatistics{count=12, sum=38.000000, min=0.000000, average=3.166667, max=9.000000}
        Delhi=DoubleSummaryStatistics{count=53, sum=175.000000, min=0.000000, average=3.301887, max=10.000000}
        Sharjah=DoubleSummaryStatistics{count=6, sum=26.000000, min=0.000000, average=4.333333, max=8.000000}
        Rajkot=DoubleSummaryStatistics{count=5, sum=31.000000, min=0.000000, average=6.200000, max=10.000000}

5. Input from the User: **select count(city) from data/ipl.csv**

        Expected Output: 
        count(city): 570
        
6. Input from the User: **select city,count(*) from data/ipl.csv group by city**

        Expected Output:
        =DoubleSummaryStatistics{count=7, sum=2870.000000, min=403.000000, average=410.000000, max=418.000000}
        Ahmedabad=DoubleSummaryStatistics{count=12, sum=4155.000000, min=121.000000, average=346.250000, max=481.000000}
        Delhi=DoubleSummaryStatistics{count=53, sum=16210.000000, min=3.000000, average=305.849057, max=576.000000}
        Sharjah=DoubleSummaryStatistics{count=6, sum=2450.000000, min=400.000000, average=408.333333, max=415.000000}
        Rajkot=DoubleSummaryStatistics{count=5, sum=2684.000000, min=523.000000, average=536.800000, max=548.000000}
        Johannesburg=DoubleSummaryStatistics{count=8, sum=807.000000, min=82.000000, average=100.875000, max=115.000000}

### Following are the broad tasks:

- Read the query from the user
- parse the query
- forward the object of query parameter to CsvQueryProcessor
- filter out rows basis on the conditions mentioned in the where clause


### Project structure

The folders and files you see in this repositories, is how it is expected to be in projects, which are submitted for automated evaluation by Hobbes

	Project
	|
	├── data 			                    // If project needs any data file, it can be found here/placed here, if data is huge they can be mounted, no need put it in your repository
	|
	├── com.stackroute.datamunger	            // all your java file will be stored in this package
	|	└── query
	|		└──parser
	|			└── AggregateFunction.java             // This class is used to store Aggregate Function
	|			└── QueryParameter.java                // This class contains the parameters and accessor/mutator methods of QueryParameter
	|			└── QueryParser.java                    // This class will parse the queryString and return an object of QueryParameter class
	|			└── Restriction.java	                // This class is for storing Restriction object
	|		└── DataSet.java 		                    // This class will be acting as the DataSet containing multiple rows
	|		└── Filter.java 		                    // This class contains methods to evaluate expressions
	|       └── GroupedDataSet.java                     // This class contains LinkedHashMap to store String and its objects
	|		└── Query.java                              // This class is used to execute the query with the help of different Query Processors and decide what type of Query processors you required.
	|	└── reader
	|		└── CsvAggregateQueryProcessor.java         // this is the CsvAggregateQueryProcessor class used for evaluating queries with aggregate functions without group by clause
	|		└── CsvGroupByQueryProcessor.java  // this is the CsvGroupByAggregateQueryProcessor class used for evaluating queries with aggregate functions and group by clause
	|		└── CsvOrderByQueryProcessor.java           // this is the CsvGroupByQueryProcessor class used for evaluating queries without aggregate functions but with group by clause
	|		└── CsvQueryProcessor.java                  // This class is used to read data from CSV file
	|		└── CsvWhereQueryProcessor.java             // This class will read from CSV file and process and return the resultSet based on Where clause 
	                                                        //Filter is user defined utility class where we defined the methods related to filter fields, filter records.
	|		└── QueryProcessingEngine.java              //The interface which has a method executeQuery(QueryParameter queryParameter) and one method getHeader with definition.
	|	└── test                                        // All your test cases are written using JUnit, these test cases can be run by selecting Run As -> JUnit Test 
	|		└── DataMungerTest.java
	|	└── DataMunger.java	                            // This is the main file, all your logic is written in this file only
	|
	├── .classpath			                            // This file is generated automatically while creating the project in eclipse
	├── .hobbes   			                    // Hobbes specific config options, such as type of evaluation schema, type of tech stack etc., Have saved a default values for convenience
	├── .project			                    // This is automatically generated by eclipse, if this file is removed your eclipse will not recognize this as your eclipse project. 
	├── pom.xml 			                    // This is a default file generated by maven, if this file is removed your project will not get recognised in hobbes.
	└── PROBLEM.md  		                    // This files describes the problem of the assignment/project, you can provide as much as information and clarification you want about the project in this file

> PS: All lint rule files are by default copied during the evaluation process, however if need to be customizing, you should copy from this repo and modify in your project repo


#### To use this as a boilerplate for your new project, you can follow these steps

1. Clone the base boilerplate in the folder **assignment-solution-step7** of your local machine
     
    `git clone https://gitlab-dev.stackroute.in/datamunger-java/step-7-Boilerplate.git assignment-solution-step7`

2. Navigate to assignment-solution-step7 folder

    `cd assignment-solution-step7`

3. Remove its remote or original reference

     `git remote rm origin`

4. Create a new repo in gitlab named `assignment-solution-step7` as private repo

5. Add your new repository reference as remote

     `git remote add origin https://gitlab.training.com/{{yourusername}}/assignment-solution-step7.git`

     **Note: {{yourusername}} should be replaced by your username from gitlab**

5. Check the status of your repo 
     
     `git status`

6. Use the following command to update the index using the current content found in the working tree, to prepare the content staged for the next commit.

     `git add .`
 
7. Commit and Push the project to git

     `git commit -a -m "Initial commit | or place your comments according to your need"`

     `git push -u origin master`

8. Check on the git repo online, if the files have been pushed


### Important instructions for Participants
> - We expect you to write the assignment on your own by following through the guidelines, learning plan, and the practice exercises
> - The code must not be plagirized, the mentors will randomly pick the submissions and may ask you to explain the solution
> - The code must be properly indented, code structure maintained as per the boilerplate and properly commented
> - Follow through the problem statement and stories shared with you

### Further Instructions on Release

*** Release 0.1.0 ***

- Right click on the Assignment select Run As -> Java Application to run your Assignment.
- Right click on the Assignment select Run As -> JUnit Test to run your Assignment.