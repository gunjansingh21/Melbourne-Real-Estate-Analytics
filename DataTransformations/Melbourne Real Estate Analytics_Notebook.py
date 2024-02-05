# Databricks notebook source
# MAGIC %md
# MAGIC **Mounting the storage account**

# COMMAND ----------

# Mounting the storage
storageAccountName = "storagemelbournehousing"
storageAccountAccessKey = 'IHbOYR65mDEYCyABzsHjwuFWSfiMcLpoYkRzn2+1JrbQnLyvE7OLyQA1diYjvtQg6CVzDBkeK6lF+ASt2U6nAw=='
sasToken = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-12-02T08:41:01Z&st=2024-02-02T00:41:01Z&spr=https&sig=FKgWU2pczyD79er0E1%2FyV3YZjPbGVwomBWUYldD6n9c%3D'
blobContainerName = "melbourne-housing"
mountPoint = "/mnt/transformData/"

# Checking if the mount point is already mounted
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    # Mounting the Azure Blob Storage to the mount point
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)


# COMMAND ----------

dbutils.fs.ls('/mnt/transformData/')

# COMMAND ----------

# MAGIC %md
# MAGIC **Loading the dataset into a Dataframe**

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Initialising spark session
spark = SparkSession.builder.appName("MelbourneRealestate").getOrCreate()

#Loading the dataset in a dataframe
path = 'dbfs:/mnt/data/melb_data.csv'
df_melb_realEstate = spark.read.csv('dbfs:/mnt/data/melb_data.csv', header=True, inferSchema=True)
display(df_melb_realEstate)


# COMMAND ----------

#Creating a dataframe with necessary rows
listOfCOlumns =['Suburb', 'Rooms', 'Bedroom2', 'Bathroom', 'Car', 'Type', 'Price', 'Landsize', 'YearBuilt', 'CouncilArea', 'Regionname']
df_melb_overview = df_melb_realEstate.select(*listOfCOlumns).withColumnRenamed('Bedroom2', 'Bedrooms').withColumnRenamed('Car', 'Parking')
display(df_melb_overview)

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating widgets for Dashboard**

# COMMAND ----------

# Getting distinct values list 
distinct_suburbs = df_melb_overview.select('Suburb').distinct()
distinct_suburbs_list = [row.Suburb for row in distinct_suburbs.collect()]

# COMMAND ----------

distinct_type = df_melb_overview.select('Type').distinct()
distinct_type_list = [row.Type for row in distinct_type.collect()]

# COMMAND ----------

distinct_bedroom = df_melb_overview.select('Bedrooms').distinct()
distinct_bedrooms_list_float = [row.Bedrooms for row in distinct_bedroom.collect()]
# Convert float values to string
distinct_bedrooms_list = [str(num) for num in distinct_bedrooms_list]

# COMMAND ----------

#Creating a dropdown widget for Suburbs
dbutils.widgets.dropdown(name = 'ListOfSuburbs', defaultValue='Melbourne', choices=distinct_suburbs_list, label='SUBURB')

# COMMAND ----------

#Creating a dropdown widget for Types of Properties
dbutils.widgets.dropdown(name = 'TypeOfProperty', defaultValue='h', choices=distinct_type_list, label='TYPE')

# COMMAND ----------

#Creating a dropdown widget for number of bedrooms
dbutils.widgets.dropdown(name = 'NumberOfBedrooms', defaultValue='2', choices=distinct_bedrooms_list, label='ROOMS')

# COMMAND ----------

display(df_melb_overview.filter((col('Suburb') == dbutils.widgets.get('ListOfSuburbs')) & 
                                (col('Type') == dbutils.widgets.get('TypeOfProperty')) & 
                                (col('Bedrooms') == dbutils.widgets.get('NumberOfBedrooms'))))

# COMMAND ----------

# MAGIC %md
# MAGIC **Overview Statistics**
# MAGIC - How many unique suburbs are covered in the dataset?
# MAGIC - What is the average price of properties in Melbourne?
# MAGIC - What is the distribution of property types (eg. house, unit, townhouse) in the dataset?

# COMMAND ----------

from pyspark.sql.functions import avg, round

#Unique suburbs
unique_suburbs = df_melb_realEstate.select('Suburb').distinct().count()
print(f"Number of unique suburbs are: {unique_suburbs}")
print("\n")

#Average price of the properties
average_price = df_melb_realEstate.select(round(avg('Price'), 0).alias('AveragePrice')).collect()[0][0]
print(f"Average price of properties is: {average_price}")
print("\n")

#Distribution of property types
property_type_distribution = df_melb_realEstate.groupBy('Type').count()
display(property_type_distribution)

# COMMAND ----------

from pyspark.sql.functions import col, avg, round

# Filtering only houses and units
houses_units_df = df_melb_realEstate.filter((col('Type') == 'h') | (col('Type') == 'u'))

# Calculating average prices for houses and units in every suburb
average_prices_per_suburb = houses_units_df.groupBy('Suburb', 'Type').agg(round(avg('Price'), 0).alias("Avg_Price"))

# Top 10 suburbs with the highest average prices for houses
average_prices_house_suburb = average_prices_per_suburb.filter(col('Type') == 'h').orderBy(col('Avg_Price').desc()).limit(10)
display(average_prices_house_suburb)

# Top 10 suburbs with the highest average prices for units
average_prices_unit_suburb = average_prices_per_suburb.filter(col('Type') == 'u').orderBy(col('Avg_Price').desc()).limit(10)
display(average_prices_unit_suburb)

# COMMAND ----------

# MAGIC %md
# MAGIC **Price Analysis**
# MAGIC - What is the average price of 3 bedroom houses?
# MAGIC - How does the price vary based on number of rooms?

# COMMAND ----------

from pyspark.sql.functions import col,desc

#Average price of 3 bedroom houses
avg_price_3_bedroomhouse = df_melb_realEstate.filter((col('Type') == 'h') & (col('Rooms') == 3)).select(round(avg('Price'), 0).alias('AveragePrice')).collect()[0][0]
print(f"Average price of 3-bedroom house is: {avg_price_3_bedroomhouse}")
print("\n")

#Price variation based on number of rooms in houses 
price_by_rooms = df_melb_realEstate.filter(col('Type') == 'h').groupBy('Rooms').agg(round(avg('Price'), 0).alias('AveragePrice')).orderBy(col('Rooms'), ascending=True)
display(price_by_rooms)

# COMMAND ----------

# MAGIC %md
# MAGIC **Selling Method Analysis**
# MAGIC - What is the most common methods of selling?
# MAGIC - How does the average price vary for different selling methods?

# COMMAND ----------

#Most common methods of selling
common_selling_method = df_melb_realEstate.groupBy('Method').count()
display(common_selling_method)

#Price variation for different selling methods
price_by_selling_method = df_melb_realEstate.groupBy('Method').agg(round(avg('Price'), 0).alias('AveragePrice'))
display(price_by_selling_method)


# COMMAND ----------

# MAGIC %md
# MAGIC **Distance from CBD analysis**
# MAGIC - How does the average price correlate with the distance from CDB?

# COMMAND ----------

from pyspark.sql.functions import desc, asc

#Distance from CBD analysis
price_distance_correlation = df_melb_realEstate.groupBy('Distance', 'Suburb').agg(round(avg('Price'), 0).alias('AveragePrice'))
price_distance_correlation_sorted = price_distance_correlation.orderBy(col('AveragePrice'), ascending=True).orderBy(col('Distance'), ascending=True).limit(20)
display(price_distance_correlation_sorted)

# COMMAND ----------

# MAGIC %md
# MAGIC **Temporal Analysis**
# MAGIC - How has the average property changed over time?
# MAGIC - Are there any seasonal trends in property sales?

# COMMAND ----------

from pyspark.sql.functions import year, month, round, avg

#Extracting year and month from date columm
df_date = df_melb_realEstate.withColumn("Year", year("Date")).withColumn("Month", month("Date"))

# Average property prices changes over time
price_change_over_time = df_date.groupBy("Year", "Month").agg(round(avg("Price")).alias("Average Price")).orderBy("Year", "Month")
display(price_change_over_time)

#Seasonal trends in property sales
seasonal_trends = df_date.groupBy('Month').count().orderBy('Month')
display(seasonal_trends)


# COMMAND ----------

# MAGIC %md
# MAGIC **Property size Analysis**
# MAGIC - What is the average land size and building area for different property types?
# MAGIC - Is there a correlation between land and property prices?

# COMMAND ----------

#Average land size and building area for different property types
size_by_property_type = df_melb_realEstate.groupBy('Type').agg(round(avg('Landsize')).alias('averageLandsize'),round(avg('BuildingArea')).alias('averageBuildarea'))
display(size_by_property_type)
print("\n")

#Correlation btw land and property prices
correlation_land_price = df_melb_realEstate.stat.corr("Landsize", "Price")
display("Correlation coefficient is - " + str(correlation_land_price))

# COMMAND ----------

# MAGIC %md
# MAGIC **Agent Performance Analysis**
# MAGIC - Identifying the top performing real estate agents based on the number of properties sold by them? 
# MAGIC - Identifying the top performing real estate agents based on the average selling price?

# COMMAND ----------

# Top performing agents based on the number of properties sold
top_agent_count = df_melb_realEstate.groupBy('SellerG').count().orderBy(col('count').desc()).limit(10)
display(top_agent_count)

# Top performing agents based on the average selling price
top_agents_price = df_melb_realEstate.groupBy("SellerG").agg(round(avg("Price"), 0).alias("avg_price")).orderBy(col('avg_price').desc())
display(top_agents_price)

# COMMAND ----------

# MAGIC %md
# MAGIC **Regional Analysis**
# MAGIC - Exploring the average property prices in different regions?
# MAGIC - Regions with the highest and lowest property prices?

# COMMAND ----------

# Average property prices in different regions
avg_price_by_region = df_melb_realEstate.groupBy('Regionname').agg(round(avg('Price'), 0).alias('avg(Price)')).orderBy(col('avg(Price)').desc())
display(avg_price_by_region)

# COMMAND ----------

#Regions with highest and lowest property prices

highest_price_region = df_melb_realEstate\
    .groupBy('RegionName', 'CouncilArea')\
    .agg(round(max('Price'), 0).alias('max(Price)'))\
    .filter(isnotnull('CouncilArea'))\
    .orderBy(col('max(Price)').desc())\
    .limit(10)
display(highest_price_region)

lowest_price_region = df_melb_realEstate\
    .groupBy('RegionName', 'CouncilArea')\
    .agg(round(min('Price'), 0).alias('min(Price)'))\
    .orderBy(col('min(Price)'))\
    .limit(10)
display(lowest_price_region)

# COMMAND ----------

# MAGIC %md
# MAGIC **Suburban Analysis**
# MAGIC - Exploring the average property prices in different suburbs?
# MAGIC - Suburbs with the highest and lowest property prices?

# COMMAND ----------

#Suburbs with highest and lowest property prices

highest_price_suburb = df_melb_realEstate.groupBy('Suburb', 'Regionname', 'Distance', 'Postcode', 'CouncilArea').agg(round(max('Price'), 0).alias('max(Price)')).orderBy(col('max(Price)').desc()).limit(4)
display(highest_price_suburb)

lowest_price_suburb = df_melb_realEstate.groupBy('Suburb', 'Regionname','Distance', 'Postcode', 'CouncilArea').agg(round(min('Price'), 0).alias('min(Price)')).orderBy(col('min(Price)')).limit(4)
display(lowest_price_suburb)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Bathroom Analysis**
# MAGIC  - Compare the average number of bathrooms for units / houses in each suburb?

# COMMAND ----------

#Average number of bathrooms for each type of property in every suburb

bathroom_analysis = df_melb_realEstate.groupBy('Suburb', 'Type').agg(round(avg('Bathroom'),0).alias("AverageBathrooms")).orderBy('Suburb', 'Type')
display(bathroom_analysis)

bathrooms_morethan2 = bathroom_analysis.filter(col('AverageBathrooms') > 2)
display(bathrooms_morethan2)


# COMMAND ----------

# MAGIC %md 
# MAGIC **Car Parking Spots Analysis**
# MAGIC  - Analyze the average number of car spaces for units / houses / townhouses in each suburb?

# COMMAND ----------

#Average number of bathrooms for each type of property in every suburb

car_parking = df_melb_realEstate.groupBy('Suburb', 'Type').agg(round(avg('Car'),0).alias("AverageCarParking")).orderBy('Suburb', 'Type')
display(car_parking)

carparking_morethan2 = car_parking.filter(col('AverageCarParking') > 2)
display(carparking_morethan2)

