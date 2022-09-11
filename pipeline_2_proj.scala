
import org.apache.spark.sql.DataFrame

def mergeGenerateCSV: DataFrame = {
    val statesPath = "brazil_covid19.csv"
    val citiesPath = "brazil_covid19_cities.csv"
    val statesDF = spark.read.option("header", "true").csv(statesPath)
    val citiesDF = spark.read.option("header", "true").csv(citiesPath)
    statesDF.createOrReplaceTempView("states")
    citiesDF.createOrReplaceTempView("cities")

    spark.sql("""
    with regions as ( select distinct state, region from states
    ), all_data as (select c.date, c.state, c.cases, c.deaths, s.region from cities c
    inner join regions s on 
    c.state = s.state)
    select date, region, state, sum(deaths) as deaths, sum(cases) as cases from all_data
    group by date, region, state
    """)
}



//show
printf("extract new csv ")

mergeGenerateCSV.coalesce(1).write.option("header", "true").csv("new_brazil_covid19.csv")

val data = mergeGenerateCSV
val statesPath = "brazil_covid19.csv"
val statesDF = spark.read.option("header", "true").csv(statesPath)

printf("compare files difference")
val diff = data.exceptAll(statesDF).toDF()


diff.coalesce(1).write.option("header", "true").csv("diff.csv")





