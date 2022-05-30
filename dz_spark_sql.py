####################      Общая часть      #####################

#В каждом задании считать, что данная часть уже выполнена

df = spark.read.option('inferSchema','true').option('header','true').csv('owid-covid-data.csv')
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import lag
from pyspark.sql.functions import round


####################      Задание №1      #####################

#Принятые допущения:
#   1. Берем дату 31.03.2021. Потому что в файле есть и 2020 год (Лучше уточнить в задании)
#   2. Честно не понял, что такое переболевшие в файле и относительно чего нужно почитать % (страны или всех)
#     Принимаем, что нужно посчитать долю заболевших к популяции населения страны (Смерти уже учтены в популяции) 
#       и вывести страны с наибольшей долей. 


#Фильтрация по дате и только странам, расчет % в новом поле, округление и вывод топа 
df.where("date == '2021-03-31' and iso_code not like 'OWID%' ").withColumn('percent',df.total_cases * 100 / df. population).select('iso_code','total_deaths','population',round(F.col('percent'),2).alias('percent')).sort(F.col('percent').desc()).show(15)


####################      Задание №2      #####################

#Датафрейм с отфильтрованными по дате и только странам данными
df1 = df.select('date','location','new_cases').where("date > '2021-03-24' and date <= '2021-03-31' and iso_code not like 'OWID%' ")
#Датафрейм с аггрегацией по стране
df2 = df1.groupBy(df1.location.alias('country')).agg(F.max('new_cases').alias('m_new_cases'),F.sum('new_cases').alias('t_new_cases'))
#Джоин датафреймов и отбор нужных полей, вывод топ 10
df1.join(df2,(df1.location == df2.country) & (df1.new_cases == df2.m_new_cases),'inner').select(df1.date,df1.location,df2.t_new_cases).sort(F.col('t_new_cases').desc()).show(10)


####################      Задание №3      #####################

#Датафрейм с отфильтрованными по дате и стране данными
df1 = df.select('date','new_cases').where((df.date >= '2021-03-24') & (df.date <= '2021-03-31') & (df.location == 'Russia'))
#Объвление окна 
w = Window().orderBy('date')
#Расчет предыдущего значения и дельты, фильтрация на null
df1.withColumn('new_cases_lag', lag('new_cases').over(w)).withColumn('delta',df.new_cases - F.col('new_cases_lag')).where("delta is not null").show()

