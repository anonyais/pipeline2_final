# Spark Pipeline Project

##  How to run

git clone `https://github.com/anonyais/pipeline2_final.git`
cd pipeline2_final

download `https://www.kaggle.com/datasets/unanimad/corona-virus-brazil?select=brazil_covid19_cities.csv` into the working directoty
download `https://www.kaggle.com/datasets/unanimad/corona-virus-brazil?select=brazil_covid19.csv`


run `spark-shell -i pipeline_2_proj.scala` 




### Approach Taken

In computing and generating `new_brazil_covid19.csv`. The two downloaded files are read into spark. And a temp table was generated. 
Then, a join was performed to get region from `brazil_covid19.csv`, so that the structure of the `new_brazil_covid19.csv`  can match the `brazil_covid19.csv`.

After the join, `brazil_covid19_cities.csv` can be said to have all necessary structure and then, a `groupby` by by date, region, state was carried out.
The `brazil_covid19.csv` and the new `new_brazil_covid19.csv` were compared using exceptAll method.



### Output

Two files were generated.
1. `new_brazil_covid19.csv`  containing the requested to be generated from `brazil_covid19_cities.csv`
2. 'diff.csv' was also generated. it contained the requested difference between the `brazil_covid19_cities.csv` and `new_brazil_covid19_cities.csv`
3. 

