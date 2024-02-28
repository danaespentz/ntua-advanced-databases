# ntua-advanced-databases
Term project for the course 'Advanced Databases' during 9th semester at NTUA

## Contributors
1. Alexandros Papanastasiou ([ntua-el17647](https://github.com/ntua-el17647))
1. Danae Spentzou ([danaespentz](https://github.com/danaespentz))


## 👣 Steps
1. Install Spark + Hadoop (see instructions at [installation](https://colab.research.google.com/drive/1eE5FXf78Vz0KmBK5W8d4EUvEFATrVLmr?usp=drive_link))

2. Run `python makedata.py`
   
3. Run `python load_data.py`
    
4. Import the data into HDFS with `hdfs dfs -mkdir /data`
   and for each file `hdfs dfs -copyFromLocal /home/user/ntua-advanced-databases/<filename> /data`

5. Exeute each query with `python query_{1,4}.py`

## Queries
1. Find, for each year, the top-3 months with highest number of recorded crimes committed. You are asked to print the month, year, number of criminal acts recorded,
   as well as the ranking of the month within the respective year. Results are expected to be sorted in ascending order with respect to the year and descending order
   with respect to the number of crimes.
2. Sort the different parts of the day taking into account crimes that were committed on the (STREET), in descending order.
3. Find the descent of the victims of recorded crimes in Los Angeles for the year 2015 in the 3 ZIP Code areas with the highest and the 3 ZIP Codes with the lowest income
   per household. Results are expected to be printed from highest to lowest number of victims per ethnic group
4. a. Calculate the number of crimes committed with the use of firearms of any kind and the average distance (in km) of the crime scene to the police station that handled the case.
   Additionally, calculate the same stats per police station.
   b. Calculate the number of crimes committed with the use of firearms of any kind and the average distance (in km) of the crime scene to the police station that is located
   closest to the crime scene. The results should appear ordered by year in ascending order. Additionally, calculate the same stats per police station.

## Notes
#### Infrastructure
- Master Node : 10.0.0.1
    - 1 Master (Spark)
    - 1 Worker (Spark)
    - 1 Namenode (HDFS)
    - 1 Datanode (HDFS)
- Worker Node : 10.0.0.2
    - 1 Worker (Spark)
    - 1 Datanode (HDFS)

#### [Spark Configuration Parameters](https://spark.apache.org/docs/latest/configuration.html)
