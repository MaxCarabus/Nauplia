# install.packages('devtools')
library(devtools)

# install.packages('DBI')
library(DBI)

setwd('/media/carabus/Apacer\ PHD/Actual/Works/plankton/scripts/2023-07-28')

devtools::install('saprobsqlite')
library(saprobsqlite)

blank = TRUE  # есть ли шаблон базы со справочниками или надо создавать заново 

speciesdic = 'Zooplankton_species_2023-03-19.xlsx' # таблица Excel со списком видов
pointdic = 'BDStvor_2023-03-03.xlsx' # таблица Excel со списком пунктов сбора (створов) 

# новая база, пустая - шаблон для наболнения баз по каждому году
connection <- dbConnect(RSQLite::SQLite(), "dics/plankton_blank.sqlite")

# если шаблона базы нет (файл: plankton_blank.sqlite) - создаём заново
if (!blank) {
  
  # справочник водных объектов
  # функцию добавления объектов надо реализовать
  waterBodiesTable(pointdic)
  
  # справочник с гидробиологическими станциями - из того же файла
  # функцию добавления объектов надо реализовать
  pointsTable(pointdic)
  
  # заполняем справочник с видами - немного медленнее, т.к. идёт проверка через GBIF Taxonomy Backbone
  speciesTable(speciesdic) # ! из локального списка надо вообще загружать
  
  filialsTable() # таблица с филиалами
  
  # таблицы с пробами, видами и обилием
  rawTables()
  
  # таблицы с расчётами
  createViews()
}

#копируем шаблон базы -> создаём для каждого года
file.copy("dics/plankton_blank.sqlite", "plankton_2018.sqlite")
file.copy("dics/plankton_blank.sqlite", "plankton_2019.sqlite")
file.copy("dics/plankton_blank.sqlite", "plankton_2020.sqlite")
file.copy("dics/plankton_blank.sqlite", "plankton_2021.sqlite")
file.copy("dics/plankton_blank.sqlite", "plankton_2022.sqlite", overwrite = T)


# если надо удалить импортированные данные
# samplePurge() - все
# samplePurge('TT') - только по одному филиалу


# ИМПОРТ ПЕРВИЧНЫХ ДАННЫХ
connection <- dbConnect(RSQLite::SQLite(), "plankton_2018.sqlite") # подключение к базе
files = c('DV_zooplankton_2018.xlsx','IR_zooplankton_2018.xlsx','KA_zooplankton_2018.xlsx',
          'KR_zooplankton_2018.xlsx','NN_zooplankton_2018.xlsx','PS_zooplankton_2018.xlsx', 
          'PZ_zooplankton_2018.xlsx','SP_zooplankton_2018.xlsx','ZB_zooplankton_2018.xlsx')
# после подключения к базе и присвоения списка видов надо запустить импорт (см. ниже)
# и все последующие команды до конца скрипта и так для каждого года

connection <- dbConnect(RSQLite::SQLite(), "plankton_2019.sqlite")
files = c('DV_zooplankton_2019.xlsx','IR_zooplankton_2019.xlsx','KA_zooplankton_2019.xlsx',
          'KR_zooplankton_2019.xlsx','NN_zooplankton_2019.xlsx','PS_zooplankton_2019.xlsx', 
          'SP_zooplankton_2019.xlsx','TT_zooplankton_2019.xlsx','ZB_zooplankton_2019.xlsx')

connection <- dbConnect(RSQLite::SQLite(), "plankton_2020.sqlite")
files = c('AR_zooplankton_2020.xlsx','DV_zooplankton_2020.xlsx','IR_zooplankton_2020.xlsx',
          'KA_zooplankton_2020.xlsx','KR_zooplankton_2020.xlsx','MM_zooplankton_2020.xlsx',
          'PS_zooplankton_2020.xlsx','SP_zooplankton_2020.xlsx','TT_zooplankton_2020.xlsx', 
          'ZB_zooplankton_2020.xlsx')

connection <- dbConnect(RSQLite::SQLite(), "plankton_2021.sqlite")
files = c('AR_zooplankton_2021-1.xlsx','AR_zooplankton_2021-2.xlsx','DV_zooplankton_2021.xlsx',
          'IR_zooplankton_2021.xlsx','KA_zooplankton_2021.xlsx','KR_zooplankton_2021.xlsx',
          'MM_zooplankton_2021.xlsx','NN_zooplankton_2021.xlsx','PS_zooplankton_2021.xlsx',
          'SP_zooplankton_2021.xlsx','TT_zooplankton_2021.xlsx','ZB_zooplankton_2021.xlsx')

connection <- dbConnect(RSQLite::SQLite(), "plankton_2022.sqlite")
files = c('AR_zooplankton_2022-1.xlsx','AR_zooplankton_2022-2.xlsx','DV_zooplankton_2022.xlsx',
          'IR_zooplankton_2022.xlsx','KA_zooplankton_2022.xlsx','MM_zooplankton_2022.xlsx',
          'NN_zooplankton_2022.xlsx','PS_zooplankton_2022.xlsx','SP_zooplankton_2022.xlsx',
          'TT_zooplankton_2022.xlsx','ZB_zooplankton_2022.xlsx')

# собственно импорт
for (i in 1:length(files)) {
  rawFile = files[i]
  importRawTable(rawFile)
}  

# данные приводим к нужной размерности - выполняем только для нужного года
# 2018
valueAdjust('NN','biomass')
valueAdjust('PZ','biomass')

# 2019
valueAdjust('NN','biomass') 

# 2020

# 2021
valueAdjust('NN','biomass')

# 2022
valueAdjust('DV','count')
valueAdjust('IR','count')
valueAdjust('NN','count')
valueAdjust('NN','biomass')
valueAdjust('ZB','count')
valueAdjust('MM','count')

# выгрузка таблицы с видами
selectToCSV('species_records_all',1)  
 
# расчёт индексов Шеннона и проч. 
# по окончании выгружается таблица с характеристиками проб
indices()
