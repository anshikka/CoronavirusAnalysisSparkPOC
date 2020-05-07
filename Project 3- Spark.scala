// Databricks notebook source
// DBTITLE 1,Introduction
// MAGIC %md
// MAGIC # COVID-19 Pandemic Analysis in Apache Spark
// MAGIC ## Ansh Sikka
// MAGIC ## Big Data Engineering and Architecture
// MAGIC ### In this project, we will use Spark to explore how different factors could have contributed to the spread of coronavirus in each country and the effects that the pandemic has left in the short term. We can possibly use this data to prepare for future pandemics or more waves of the virus.

// COMMAND ----------

// DBTITLE 1,Data
// MAGIC %md
// MAGIC ## Datasets
// MAGIC * COVID-19 Case Data (https://data.world/covid-19-data-resource-hub/covid-19-case-counts) (Updated 2020)
// MAGIC * World Tourism Data (https://data.worldbank.org/indicator/st.int.arvl) (Updated 2018)
// MAGIC * World Healthcare Expenditure Data (https://data.worldbank.org/indicator/SH.XPD.CHEX.PC.CD) (Updated 2016)
// MAGIC 
// MAGIC I have put all of these datasets in an S3 Bucket for Public Use

// COMMAND ----------

// DBTITLE 1,COVID-19 Case Dataset Details
// MAGIC %md
// MAGIC The dataset contains 18 columns. The ones that are bolded are the ones that we will use for our analysis.
// MAGIC * **Case_type: Confirmed Cases and total deaths**
// MAGIC * **Number of Cases: Point in time snapshot of to-date totals (i.e., Mar 22 is inclusive of all prior dates)**
// MAGIC * **Date: Jan 23, 2020 - Present**
// MAGIC * Combined_Key: Name of country
// MAGIC * Country_region: Provided for all countries
// MAGIC * Province_state: Provided for Australia, Canada, China, Denmark, France, Netherlands, United Kingdom, United States
// MAGIC * Admin2: US only - County name
// MAGIC * iso2: 2-Digit Country Code
// MAGIC * **iso3: 3-Digit Country Code**
// MAGIC * FIPS: US only - 5-digit Federal Information Processing Standard
// MAGIC * Combined_Key: US only - Combination of Admin 2, State_Province, and Country_Region
// MAGIC * Lat
// MAGIC * Long
// MAGIC * **Population_Count: Number of people in country**
// MAGIC * People_Total_Tested: Number of people tested (*There were zero values in this column *)
// MAGIC * People_Total_Hopsitalized: Number of people hospitalized (*There were zero values in this column *)

// COMMAND ----------

// DBTITLE 1,Tourism (Arrivals Per Country) Dataset Details
// MAGIC %md
// MAGIC The data contains 64 columns. The ones that are bolded are the ones that we will use for our analysis. The only issue that might affect accuracy in analysis with this data is that it only goes up to 2018. However, we can make an assumption that the popular destinations for tourisms/arrivals haven't changed that drastically in the past 2 years.
// MAGIC 
// MAGIC * Country Name: Provided name for all countries
// MAGIC * **Country Code: 3-Digit Country Code**
// MAGIC * Indicator Name: All are international arrivals
// MAGIC * Indicator Code: All are `ST.INT.ARVL`
// MAGIC * Year Columns -> Each contain the number of arrivals for each country, including total world arrival counts.
// MAGIC   * 1964-1994 Columns are Null
// MAGIC   * 1994-2018 Columns are not Null
// MAGIC   * **We will be using 2018 Data**

// COMMAND ----------

// DBTITLE 1,Healthcare (Expenditure By Country) Dataset Details
// MAGIC %md
// MAGIC The data contains 64 columns. The ones that are bolded are the ones that we will use for our analysis. The only issue that might affect accuracy in analysis with this data is that it only goes up to 2016, so further representations may not be completely accurate.
// MAGIC 
// MAGIC * Country Name: Provided name for all countries
// MAGIC * **Country Code: 3-Digit Country Code**
// MAGIC * Indicator Name: Current Healthcare Expenditure (% of GDP)
// MAGIC * Indicator Code: All are `SH.XPD.CHEX.GD.ZS`
// MAGIC * Year Columns -> Each contain the number of arrivals for each country, including total world arrival counts.
// MAGIC   * 1960-2000, 2017-2019 Columns are Null
// MAGIC   * 2000-2016 Columns are not Null
// MAGIC   * **We will be using 2016 Data**

// COMMAND ----------

// DBTITLE 1,Delete Existing Datasets
// MAGIC %sh 
// MAGIC # delete existing datasets
// MAGIC rm -r datasets

// COMMAND ----------

// DBTITLE 1,Download Datasets from Pre-Existing S3 Bucket
// MAGIC %sh 
// MAGIC # Download datsets from S3 Bucket (Public)
// MAGIC wget https://covid-19-cause-and-effect-analysis-2020-ansh-sikka.s3-us-west-2.amazonaws.com/published/1587428318/COVID-19+Cases.csv
// MAGIC wget https://covid-19-cause-and-effect-analysis-2020-ansh-sikka.s3-us-west-2.amazonaws.com/published/1587428318/API_ST.INT.ARVL_DS2_en_csv_v2_937675.csv
// MAGIC wget https://covid-19-cause-and-effect-analysis-2020-ansh-sikka.s3-us-west-2.amazonaws.com/published/1587428318/API_SH.XPD.CHEX.GD.ZS_DS2_en_csv_v2_989101.csv

// COMMAND ----------

// DBTITLE 1,Verify Datasets are Downloaded
// MAGIC %sh
// MAGIC # list files
// MAGIC ls

// COMMAND ----------

// DBTITLE 1,Rename Datasets for Easier Usage
// MAGIC %sh
// MAGIC 
// MAGIC # move all to datasets cluster folder
// MAGIC mkdir datasets
// MAGIC mv API_ST.INT.ARVL_DS2_en_csv_v2_937675.csv datasets/arrivals_per_country.csv
// MAGIC mv COVID-19+Cases.csv datasets/covid_19_cases_by_country.csv
// MAGIC mv API_SH.XPD.CHEX.GD.ZS_DS2_en_csv_v2_989101.csv datasets/health_expenditure_by_country.csv

// COMMAND ----------

// DBTITLE 1,Verify Datasets are Renamed and CSV types
// MAGIC %sh
// MAGIC # for each dataset, describe metadata about the file
// MAGIC ls datasets
// MAGIC echo 
// MAGIC echo "--Types--"
// MAGIC for file in datasets/*
// MAGIC   do
// MAGIC     echo File Details for "$file"
// MAGIC     file $file
// MAGIC done

// COMMAND ----------

// DBTITLE 1,Remove Previous Directories Created on Databricks Filesystem
// create folder and display
dbutils.fs.rm("pandemic-2020-datasets", recurse = true);
display(dbutils.fs.ls("dbfs:/"))

// COMMAND ----------

// DBTITLE 1,Create Directories in Databricks Filesystem
// Create folder for each dataset inside a main 'pandemic-2020-datasets' folder
dbutils.fs.mkdirs("pandemic-2020-datasets")
dbutils.fs.mkdirs("/pandemic-2020-datasets/arrivals_per_country")
dbutils.fs.mkdirs("/pandemic-2020-datasets/covid_19_cases_by_country");
dbutils.fs.mkdirs("/pandemic-2020-datasets/health_expenditure_by_country")
display(dbutils.fs.ls("dbfs:/pandemic-2020-datasets/"))

// COMMAND ----------

// DBTITLE 1,Copy Datasets from Main Filesystem to Databricks Filesystem
dbutils.fs.cp("file:/databricks/driver/datasets/arrivals_per_country.csv", "dbfs:/pandemic-2020-datasets/arrivals_per_country/")
dbutils.fs.cp("file:/databricks/driver/datasets/covid_19_cases_by_country.csv", "dbfs:/pandemic-2020-datasets/covid_19_cases_by_country/")
dbutils.fs.cp("file:/databricks/driver/datasets/health_expenditure_by_country.csv", "dbfs:/pandemic-2020-datasets/health_expenditure_by_country/")

// COMMAND ----------

// MAGIC %md
// MAGIC Since there are many columns in all 3 of the datasets, we will infer the schema. We will then verify the schema once the data is filtered and clean to maintain validity.

// COMMAND ----------

// DBTITLE 1,Load COVID-19, Economic, and Tourism Data into Spark Using Scala
// For each of these, we will infer the schema since there are too many columns. We will then select the columns we need from each dataset and verify that the schema is correct

// COVID-19 Dataset
val covid_19_df_raw = spark.read.format("csv")
.option("inferschema", "true")
.option("header", "true")
.option("delimeter", ",")
.load("dbfs:/pandemic-2020-datasets/covid_19_cases_by_country/covid_19_cases_by_country.csv")

// Tourism Dataset
val tourism_df_raw = spark.read.format("csv")
.option("inferschema", "true")
.option("header", "true")
.option("delimiter", ",")
.load("dbfs:/pandemic-2020-datasets/arrivals_per_country/arrivals_per_country.csv")

// Health Expenditure Dataset
val health_expenditure_df_raw = spark.read.format("csv")
.option("inferschema", "true")
.option("header", "true")
.option("delimeter", ",")
.load("dbfs:/pandemic-2020-datasets/health_expenditure_by_country/health_expenditure_by_country.csv");

// COMMAND ----------

// DBTITLE 1,Cleaning COVID-19 Data
// List columns
val covid_19_df_columns_raw = covid_19_df_raw.columns;
covid_19_df_columns_raw.foreach {println}

// COMMAND ----------

// DBTITLE 1,COVID-19 Dataset Columns For Analysis
// MAGIC %md
// MAGIC So we will need
// MAGIC * Case_Type: `String`
// MAGIC * Cases: `int`
// MAGIC * iso3: `String`
// MAGIC * Population_Count: `int`
// MAGIC * Date: `date`
// MAGIC 
// MAGIC 
// MAGIC We can drop the rest of the columns. Let's also delete any rows with null number of cases, iso3 codes, or no population count.
// MAGIC Since we infered our schema (there were many columns), we should verify that our columns are the correct datatype.

// COMMAND ----------

// DBTITLE 1,COVID-19 Dataset Filtering
import org.apache.spark.sql.functions.to_date

val filtered_covid_19_df = covid_19_df_raw.select("Case_Type", "Cases", "iso3", "Population_Count", "Date"); // select columns needed and create new dataframe with columns
val filtered_covid_19_df_with_date = filtered_covid_19_df.withColumn("Date", to_date($"Date", "MM/dd/yyyy")); // convert to date format in schema
val filtered_covid_19_df_no_null = filtered_covid_19_df_with_date.na.drop(); // drop null rows
filtered_covid_19_df_no_null.explain(); // show plan

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's see what the data looks like and see how we can organize it for efficient analysis.
// MAGIC We will also count the number of countries we have to consider for future grouping and aggregations

// COMMAND ----------

// DBTITLE 1,Verifying Dataset Filtering
filtered_covid_19_df_no_null.show(50); // show 50 results
filtered_covid_19_df_no_null.selectExpr("count(distinct(iso3)) AS Number_Of_Countries").show(); // total number of countries represented

// COMMAND ----------

// MAGIC %md
// MAGIC Since we have data that shows deaths and confirmed cases, let's split the dataset into deaths and confirmed cases

// COMMAND ----------

// DBTITLE 1,Splitting COVID-19 Dataset into Deaths and Cases
// split dataframe into 2
val covid_19_cases_df = filtered_covid_19_df_no_null.where("Case_Type = 'Confirmed'");
val covid_19_deaths_df = filtered_covid_19_df_no_null.where("Case_Type = 'Deaths'").withColumnRenamed("Cases", "Deaths");

// verify types
covid_19_cases_df.printSchema();
covid_19_deaths_df.printSchema(); 

// COMMAND ----------

// DBTITLE 1,Cleaning Tourism Data
val tourism_df_columns_raw = tourism_df_raw.columns;
tourism_df_columns_raw.foreach {println}

// COMMAND ----------

// MAGIC %md
// MAGIC So we will need
// MAGIC * Country Code: `String`
// MAGIC * 2018: `double` --> This is a double since numbers are high enough to utilize exponents
// MAGIC 
// MAGIC We can drop the rest of the columns. Let's also delete any rows with null number of arrivals or null country codes.

// COMMAND ----------

// DBTITLE 1,Filtering Tourism Dataset
val filtered_tourism_df = tourism_df_raw.select("Country Code", "2018").withColumnRenamed("Country Code","iso3") // select only columns needed and build new dataframe
val filtered_tourism_df_no_null = filtered_tourism_df.na.drop(); // drop null rows
val tourism_df = filtered_tourism_df_no_null; // set cleaned tourism dataframe
tourism_df.explain(); // show plan

// COMMAND ----------

// DBTITLE 1,Verifying Filtered Tourism Dataset
tourism_df.show(50); // show 50 rows 
tourism_df.selectExpr("count(distinct(iso3)) AS Number_Of_Countries").show(); // show number of countries accounted for in dataset

// COMMAND ----------

// DBTITLE 1,Cleaning Healthcare Expenditure Data
// List Columns
val health_expenditure_df_columns_raw = health_expenditure_df_raw.columns;
health_expenditure_df_columns_raw.foreach {println}

// COMMAND ----------

// MAGIC %md
// MAGIC So we will need
// MAGIC * Country Code: `String`
// MAGIC * 2016: `double`
// MAGIC 
// MAGIC We can drop the rest of the columns. Let's also delete any rows with null number of arrivals or null country codes.

// COMMAND ----------

// DBTITLE 1,Filtering Health Expenditure Data
val filtered_health_expenditure_df = health_expenditure_df_raw.select("Country Code", "2016").withColumnRenamed("Country Code","iso3")
val filtered_health_expenditure_df_no_null = filtered_health_expenditure_df.na.drop();
filtered_health_expenditure_df_no_null.columns.foreach {println}
val health_expenditure_df = filtered_health_expenditure_df_no_null;
health_expenditure_df.printSchema();
health_expenditure_df.explain();

// COMMAND ----------

// DBTITLE 1,Verifying Health Expenditure Dataset
health_expenditure_df.show(50);
health_expenditure_df.selectExpr("count(distinct(iso3)) AS Number_Of_Countries").show();

// COMMAND ----------

// MAGIC %md
// MAGIC All 3 datasets are ready for analysis. They will be joined on iso3 (country code).

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1: Does the population of a country affect the number of confirmed cases and deaths? ###
// MAGIC To manipulate our data, we will 
// MAGIC * Filter the dataframes for the number of cases and deaths to date and remove all other rows. In this case, the latest date for the dataset is 04/19/2020
// MAGIC * Create a temporary Spark SQL view for the latest COVID-19 Cases
// MAGIC * Aggregate the number of cases (avg) and population (sum) by ISO-3 Country Code --> We will save this aggregation as a dataframe as a since we will use it for further analysis
// MAGIC * Aggregate the number of death (avg) and population (sum) by ISO-3 Country Code --> We will save this aggregation as a dataframe as a since we will use it for further analysis

// COMMAND ----------

// DBTITLE 1,Population vs. Cases
import org.apache.spark.sql.functions._

// get the latest date from the dataset (up until 04/19/2020)
val latest_date_filter = col("date") === lit("2020-04-19"); // predefined filter
val latest_covid_19_cases_df = covid_19_cases_df.filter(latest_date_filter);
val latest_covid_19_deaths_df = covid_19_deaths_df.filter(latest_date_filter);

// create temporary views for deaths and cases
latest_covid_19_cases_df.createOrReplaceTempView("latest_covid_19_cases_table")
latest_covid_19_deaths_df.createOrReplaceTempView("latest_covid_19_deaths_table");

// produce seperate dataframes for aggregated case by country data
val latest_covid_19_cases_per_country_df = spark.sql("SELECT iso3, SUM(Cases) AS Number_Of_Cases, SUM(Population_Count) as Population FROM latest_covid_19_cases_table GROUP BY iso3 ORDER BY Number_Of_Cases DESC");
latest_covid_19_cases_per_country_df.explain();

/* Graph a scatter plot to show population vs. number of cases
 * PLOT OPTIONS
 * TYPE: Scatter Plot
 * Keys: 
 * Series Groupings:
 * Values: [Population, Number_Of_Cases]
 */
display(latest_covid_19_cases_per_country_df)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the number of deaths vs. the population up until 04/19/2020. 

// COMMAND ----------

// DBTITLE 1,Population vs. Deaths
val latest_covid_19_deaths_per_country_df = spark.sql("SELECT iso3, SUM(Deaths) AS Number_Of_Deaths, SUM(Population_Count) as Population FROM latest_covid_19_deaths_table GROUP BY iso3 ORDER BY Number_Of_Deaths DESC");

/* Graph a scatter plot to show population vs. number of deaths
 * PLOT OPTIONS
 * TYPE: Scatter Plot
 * Keys: 
 * Series Groupings:
 * Values: [Population, Number_Of_Deaths]
 */
display(latest_covid_19_deaths_per_country_df)

// COMMAND ----------

// DBTITLE 1,Correlations between Populations and Cases/Deaths
// How correlated is population size and the number of case?
val cases_population_corr = latest_covid_19_cases_per_country_df.stat.corr("Population", "Number_Of_Cases")
val deaths_population_corr = latest_covid_19_deaths_per_country_df.stat.corr("Population", "Number_Of_Deaths")
println("The correlation between population and the number of cases is: " + cases_population_corr);
println("The correlation between population and the number of deaths is: " + deaths_population_corr);

// COMMAND ----------

// MAGIC %md
// MAGIC As we can see, there is a *very small positive correlation between population size and the number of cases (**0.236**)*. Additionally, there is even a *smaller positive correlation between population size and number of deaths attributed to COVID-19 (**0.19**)* Looking at the graphs, we can see a lot of countries with higher populations don't have that many cases or deaths either. However, what is that one outlier on the top of the 200M-400M population size? You guessed it, that's the **USA**. We can see that the USA is a breeding ground for this. We *cannot* accurately predict the number of cases or deaths by the country's population. The true number of cases would also depend on different factors like availablity of testing (this is where the USA leads), healthcare system, tourism, etc. We will explore some of these factors next.

// COMMAND ----------

// DBTITLE 1,A quick pie chart to see distribution of cases by country
/* Graph a pie graph to show distribution of cases by country
 * PLOT OPTIONS
 * TYPE: Pie Chart
 * Keys: [iso3]
 * Series Groupings:
 * Values: [Number_Of_Cases]
 */

display(latest_covid_19_cases_per_country_df)

// COMMAND ----------

latest_covid_19_cases_per_country_df.select(skewness("Number_Of_Cases")).show(); // calculate skewness of number of cases distribution by country

// COMMAND ----------

// MAGIC %md
// MAGIC Woah, the skewness is pretty strong on the number of cases. Looking at the visualization below, we can (again) easily confirm that the USA is the root cause

// COMMAND ----------

/* Graph a bar chart to show number of cases per country
 * PLOT OPTIONS
 * TYPE: Bar Chart
 * Keys: [iso3]
 * Series Groupings:
 * Values: [Number_Of_Cases]
 */
display(latest_covid_19_cases_per_country_df);

// COMMAND ----------

// DBTITLE 1,A quick pie chart to see distribution of deaths by country
/* Graph a pie graph to show distribution of deaths by country
 * PLOT OPTIONS
 * TYPE: Pie Chart
 * Keys: [iso3]
 * Series Groupings:
 * Values: [Number_Of_Deaths]
 */
display(latest_covid_19_deaths_per_country_df)

// COMMAND ----------

// MAGIC %md
// MAGIC It's pretty obvious to see that the USA has the most cases and deaths (as we've seen on the news). This would explain the outliers in both of the previous scatter plots. But why is this? Is it the healthcare system? Let's examine the healthcare systems of the countries and maybe see if there is a correlation between focus on health and number of cases/deaths.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2: Does Healthcare Expenditure Affect the Number of Cases and Deaths? ###
// MAGIC To manipulate our data, we will 
// MAGIC * Create a temporary Spark SQL table for 2016 Health Expenditure by Country
// MAGIC * Join the filtered COVID-19 Cases Temporary Table and Health Expenditure Temporary Table by iso3 (Country Code) -> *We will be performing an inner join since we don't want to include countries that appear in one dataset but not the other.*
// MAGIC * Aggregate the number of cases and arrivals (sum) by ISO-3 Country Code

// COMMAND ----------

// DBTITLE 1,Health Expenditure vs. Cases
val health_expenditure_cases_df_join = latest_covid_19_cases_per_country_df.col("iso3") === health_expenditure_df.col("iso3"); // join on country code
val health_expenditure_cases_df = latest_covid_19_cases_per_country_df.join(health_expenditure_df, health_expenditure_cases_df_join).select("latest_covid_19_cases_table.iso3", "Number_Of_Cases", "2016").withColumnRenamed("2016", "Health Expenditure Percentage of GDP in 2016");

/* Graph a scatter plot to show number show health expenditure (% of GDP) vs. number of cases
 * PLOT OPTIONS
 * TYPE: Scatter Plot
 * Keys:
 * Series Groupings:
 * Values: [Health Expenditure Percentage of GDP in 2016, Number_Of_Cases]
 */
display(health_expenditure_cases_df)

// COMMAND ----------

// MAGIC %md
// MAGIC Hm, we can see the number of cases do tend to increase with the health expenditure percentage. This might be due to outdated data. However, we can assume that healthcare expenditure won't always affect the number of people infected. A possible explanation for this might be the fact that most people get treated for the virus back at home. I better way to view the effect would be looking at the death rates. This way we can see how well the healthcare system would respond to these sick patients.

// COMMAND ----------

// DBTITLE 1,Health Expenditure vs. Deaths
val health_expenditure_deaths_df_join = latest_covid_19_deaths_per_country_df.col("iso3") === health_expenditure_df.col("iso3"); // join on country code
val health_expenditure_deaths_df = latest_covid_19_deaths_per_country_df.join(health_expenditure_df, health_expenditure_deaths_df_join).select("latest_covid_19_deaths_table.iso3", "Number_Of_Deaths", "2016").withColumnRenamed("2016", "Health Expenditure Percentage of GDP in 2016");

/* Graph a scatter plot to show number show health expenditure (% of GDP) vs. number of deaths
 * PLOT OPTIONS
 * TYPE: Scatter Plot
 * Keys:
 * Series Groupings:
 * Values: [Health Expenditure Percentage of GDP in 2016, Number_Of_Deaths]
 */
display(health_expenditure_deaths_df);

// COMMAND ----------

// DBTITLE 1,Correlation between Health Expenditure and Cases/Deaths
import org.apache.spark.sql.functions._

// How correlated is health expenditure and the number of cases? What about the number of deaths?
val cases_health_expenditure_corr = health_expenditure_cases_df.stat.corr("Health Expenditure Percentage of GDP in 2016", "Number_Of_Cases")
val deaths_health_expenditure_corr = health_expenditure_deaths_df.stat.corr("Health Expenditure Percentage of GDP in 2016", "Number_Of_Deaths")
println("The correlation between the health expenditure and the number of cases is: " + cases_health_expenditure_corr);
println("The correlation between the number of health expenditure and the number of deaths is: " + deaths_health_expenditure_corr);

// COMMAND ----------

// MAGIC %md
// MAGIC We can see that there is *a positive correlation (**0.40**) for both case rates and death rates.* This means that as health expenditure tends to increase, so does the rate of infections and rate of deaths. Some reasons why this may be is that people take a society's health system for granted and don't monitor their health as much. We cannot be completely accurate about the data since the health expenditure data is from 2016. There must be another reson of why infection rates are so high. Let's see how it spreads, since this disease *is* decently contagious.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3: Does Country Arrival Amount Affect the Number of Cases and Deaths? (Tracking Spread) ###
// MAGIC To manipulate our data, we will 
// MAGIC * Create a temporary Spark SQL table for 2018 Arrivals-by-Country
// MAGIC * Join the filtered COVID-19 Cases Temporary Table and Tourism Temporary Table by iso3 (Country Code) -> *We will be performing an inner join since we don't want to include countries that appear in one dataset but not the other.*
// MAGIC * Aggregate the number of cases and arrivals (sum) by ISO-3 Country Code

// COMMAND ----------

// DBTITLE 1,Arrival Amount vs. Cases
val tourism_cases_df_join = latest_covid_19_cases_per_country_df.col("iso3") === tourism_df.col("iso3"); // join on country code
val tourism_cases_df = latest_covid_19_cases_per_country_df.join(tourism_df, tourism_cases_df_join).select("latest_covid_19_cases_table.iso3", "Number_Of_Cases", "2018").withColumnRenamed("2018", "Number of Arrivals in 2018");

/* Graph a scatter plot to show number show number of arrivals per country vs. number of cases
 * PLOT OPTIONS
 * TYPE: Scatter Plot
 * Keys:
 * Series Groupings:
 * Values: [Number of Arrivals in 2018, Number_Of_Cases]
 */
display(tourism_cases_df)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the number of deaths vs. the average tourism arrival amount

// COMMAND ----------

// DBTITLE 1,Arrival Amount vs. Deaths
val tourism_deaths_df_join = latest_covid_19_deaths_per_country_df.col("iso3") === tourism_df.col("iso3"); // join on country code
val tourism_deaths_df = latest_covid_19_deaths_per_country_df.join(tourism_df, tourism_deaths_df_join).select("latest_covid_19_deaths_table.iso3", "Number_Of_Deaths", "2018").withColumnRenamed("2018", "Number of Arrivals in 2018");

/* Graph a scatter plot to show number show number of arrivals per country vs. number of deaths
 * PLOT OPTIONS
 * TYPE: Scatter Plot
 * Keys:
 * Series Groupings:
 * Values: [Number of Arrivals in 2018, Number_Of_Deaths]
 */
display(tourism_deaths_df)

// COMMAND ----------

// DBTITLE 1,Correlation between Travel/Tourism and Cases and Deaths
import org.apache.spark.sql.functions._

// How correlated is number of arrivals and the number of cases? What about the number of deaths?
val cases_arrivals_corr = tourism_cases_df.stat.corr("Number of Arrivals in 2018", "Number_Of_Cases")
val deaths_arrivals_corr = tourism_deaths_df.stat.corr("Number of Arrivals in 2018", "Number_Of_Deaths")
println("The correlation between the number of arrivals (tourism) and the number of cases is: " + cases_arrivals_corr);
println("The correlation between the number of arrivals (tourism) and the number of deaths is: " + deaths_arrivals_corr);

// COMMAND ----------

// MAGIC %md
// MAGIC Woah, now we see that the correlation is way higher than before. A correlation coefficient of **0.67** and **0.75** for cases and deaths based on average arrivals means that there might be some kind of pattern in travel and disease spread. We can't be completely accurate since the travel data is from 2018, but popular tourism destinations don't change as easily. So far, we can see that countries with high populations don't really affect the number of cases as much (then again, there are a bunch of other factors). However, we can see that maybe the movement of people throughout the world has an effect on the spread. This can be useful for future pandemics because authorities can halt travel to certain popular destinations immediately to slow down the spread.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Have we hit our peak? ###
// MAGIC As of now, it seems that cases are still climbing worldwide.

// COMMAND ----------

/* Graph a bar chart to display number of COVID-19 cases over time
 * PLOT OPTIONS
 * TYPE: Bar Chart
 * Keys: [Date]
 * Series Groupings:
 * Values: [Cases]
 */
display(covid_19_cases_df)

// COMMAND ----------

/* Graph a bar chart to display number of COVID-19 deaths over time
 * PLOT OPTIONS
 * TYPE: Bar Chart
 * Keys: [Date]
 * Series Groupings:
 * Values: [Deaths]
 */
display(covid_19_deaths_df)

// COMMAND ----------

// MAGIC %md
// MAGIC Unfortunately, the number of cases rose every time, but let's take a more optimistic look at this. We want to know the number of new cases that come up everyday since the data started being recorded.
// MAGIC Since the data only includes cummulative counts of cases up to a date, we have to calculate the number of new cases by using a **window** function. This will allow us to specify an interval to scan data over. In this case, the window would just be the previous day. We then create a **lag** filter that will show a previous row's value (in this case, the previous day's number of cases). We then create another column that shows the difference between the previous days cases and the current days cases using the lag function. 

// COMMAND ----------

import org.apache.spark.sql.expressions._;

covid_19_cases_df.createOrReplaceTempView("covid_19_cases_table") // create a temporary spark view
val covid_19_cases_worldwide_df = spark.sql("SELECT date AS Date, SUM(Cases) AS Number_Of_Cases FROM covid_19_cases_table GROUP BY date ORDER BY date"); // only need date and cases
covid_19_cases_worldwide_df.show();
val date_window = Window.orderBy("Date") // interval is day by day                                  
val previous_day = (lag(col("Number_Of_Cases"), 1).over(date_window)) // lag over the previous day (1 day)
val covid_19_cases_worldwide_change_over_time_df = covid_19_cases_worldwide_df.withColumn("Number of New Cases", (col("Number_Of_Cases") - previous_day)) // new dataframe with column showing change from lag and window function

/* Graph a bar chart to display number of new COVID-19 cases over time
 * PLOT OPTIONS
 * TYPE: Bar Chart
 * Keys: [Date]
 * Series Groupings:
 * Values: [Number of New Cases]
 */
display(covid_19_cases_worldwide_change_over_time_df);


// COMMAND ----------

// MAGIC %md
// MAGIC This is a good way to see how the curve is actually responding to the pandemic. Looking currently, we can see that the new number of cases is actually starting to decrease, giving us an optimistic view. We can't confirm though, since this can just be a temporary dip. 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Conclusion ##
// MAGIC As we can see, a lot of factors can influence a pandemic. The best thing to do is take preventative measures. The biggest pattern seen was from the tourism data, since a lot of disease *did* spread through travel. It was also surprising to see that things like travel and health expenditure didn't have as large of an effect than people assume. Then again, the velocity of this data isn't the best. However, using historical data puts some kind of control into predicting and taking measures into what governments should do to stop the spread of disease. 

// COMMAND ----------


