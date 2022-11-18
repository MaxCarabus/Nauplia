# install.packages('devtools')
library(devtools)
library(DBI)
setwd('/media/carabus/Transcend/investigation/data_science/bdi/plankton/scripts/2022-10-09')

devtools::install('saprobsqlite')
library(saprobsqlite)

blank = FALSE # есть ли шаблон базы со справочниками или надо создавать заново 

# новая база
connection <- dbConnect(RSQLite::SQLite(), "plankton_2022-10-10.sqlite")

if (!blank) {
  
  # справочник водных объектов
  # функцию добавления объектов надо реализовать
  waterBodiesTable('BDStvor_2022-10-09.xlsx')
  
  # справочник с гидробиологическими станциями - из того же файла
  # функцию добавления объектов надо реализовать
  pointsTable('BDStvor_2022-10-09.xlsx')
  
  # заполняем справочник с видами - немного медленнее, т.к. идёт проверка через GBIF Taxonomy Backbone
  speciesTable('Zooplankton_species_2022-06-05.xlsx')
  # ! из локального списка надо вообще загружать
  
  # таблица с филиалами
  filialsTable()
  
  # таблицы с пробами, видами и обилием
  rawTables()
  
  # таблицы с расчётами
  createViews()
}

# ИМПОРТ ПЕРВИЧНЫХ ДАННЫХ
files = c('AR_zooplankton_2021-1.xlsx', 'AR_zooplankton_2021-2.xlsx',
          'DV_zooplankton_2021.xlsx',   'IR_zooplankton_2021.xlsx',
          'KA_zooplankton_2021.xlsx',   'KR_zooplankton_2021.xlsx',
          'MM_zooplankton_2021.xlsx',   'NN_zooplankton_2021.xlsx',
          'PS_zooplankton_2021.xlsx', 'SP_zooplankton_2021.xlsx',
          'TT_zooplankton_2021.xlsx',   'ZB_zooplankton_2021.xlsx')

# files = c('KA_zooplankton_2021.xlsx')

# если надо записи удалить
# samplePurge() - все
# samplePurge('TT') - только по одному филиалу

# собственно импорт
for (i in 1:length(files)) {
  rawFile = files[i]
  importRawTable(rawFile)
}  


# пересчёт значений на кубометр
# отключено за ненадобностью
# update = 'UPDATE occurrences SET biomass_m3 = biomass*10 WHERE biomass_m3 IS NULL;'
# dbExecute(connection,update)
# update = "UPDATE occurrences SET count_m3 = ex_count*0.1/100 WHERE count_m3 IS NULL;"
# update = "UPDATE occurrences SET count_m3 = ex_count*10 WHERE count_m3 IS NULL;"
# dbExecute(connection,update)


# выгрузка таблицы с видами 
selectToCSV('species_records_all',1)
# расчёт индексов Шеннона и проч.
indices()
verbatimDistinct()


#### постобработка
setwd('/media/carabus/Transcend/investigation/data_science/bdi/plankton/scripts/2022-07-02')
samplesSum = read.csv('sample_summary_dump_2022-07-03.csv')
colnames(samplesSum)
head(samplesSum)
points = samplesSum[,c(3:10,14)]
points0 = samplesSum[,c(3:10)]
nrow(points0)
nrow(unique(points0))


colnames(points)
head(points)
nrow(points)
pointsDistinct = unique(points)
nrow(pointsDistinct)
write.csv(pointsDistinct,'unique_points.csv')

bblist = read.csv('BackBoneBuffer.csv')
bblist = data.frame(backbone_key = 1, taxon_rank = 'KINGDOM', sp_verbatim = 'Animalia', sp_backbone = 'Animalia')
bblist = rbind(bblist, data.frame(spKey,rank,sp,spbb))
spKey = 2234705
rank = 'SPECIES'
sp = 'Bosmina coregoni'
spbb = 'Bosmina coregoni Baird, 1857'

bblist = rbind(bblist, data.frame(backbone_key=spKey,taxon_rank=rank,sp_verbatim=sp,sp_backbone=spbb))

# ищём "ложные" пробы
setwd('/media/carabus/Transcend/investigation/data_science/bdi/plankton/scripts/2022-09-08')
samples = read.csv('sample_summary_dump_2022-09-08.csv')
nrow(samples)
colnames(samples)

# отбираем поля для поиска
smples = unique(samples[,c('filial','trunk_code','sampling_date','sampling_time_min',
                    'vertical','horizon_min','temperature','instrument')])


nrow(smples)
colnames(smples)
head(smples)
smpl = smples[order(smples$trunk_code,smples$sampling_date,smples$sampling_time_min,smples$vertical,smples$horizon_min),]

head(smpl)
write.csv(smpl,'samples_ordered.csv')

smpl[smpl$trunk_code == '1605303' 
     # & smpl$sampling_date == '2021-08-16'
     # & smpl$sampling_time_min == sm$sampling_time_min
     # & smpl$vertical == '0.50'
     # & smpl$horizon_min == '0'
     ,]


n = nrow(smpl)
for (i in 1:n) {
  sm = smpl[i,]
  # различаются только временем и/или температурой
  selected = smpl[smpl$trunk_code == sm$trunk_code 
                  & smpl$sampling_date == sm$sampling_date 
                  # & smpl$sampling_time_min == sm$sampling_time_min
                  & smpl$vertical == sm$vertical
                  & smpl$horizon_min == sm$horizon_min,]
  
  nmatch = nrow(selected)
  if (nmatch > 1) {
    cat(paste0('Номер пробы: ',i,' число совпадений - ',nmatch,'\n'))
  }
}


fileNames <- list.files(path = 'raw', pattern = '.csv')
l = length(fileNames)
data = data.frame(matrix(ncol=12, nrow=0))
for (i in 1:l) {
 colnames(data) = colnames(read.csv(paste0('raw/',fileNames[i])))
 data = rbind(data, read.csv(paste0('raw/',fileNames[i])))
}
data[1] = NULL
colnames(data)
data[1] = NULL
data[1] = NULL
data[2] = NULL
head(data)
data$trunk_code = data$Код.пункта.по.ГСН*100 + data$Створ
colnames(data)
data[1] = NULL
data[1] = NULL
data$sampling_date = data$Дата.отбора
data$sampling_time = data$Время.отбора
data$vertical = data$Вертикаль
data$vertical[is.na(data$vertical)] = 99
data$horizon = data$Горизонт
data$temperature = data$Т.С.воды
data$instrument = data$Орудие.лова
head(data)
data[1] = NULL
data[1] = NULL
data[1] = NULL
data[1] = NULL
data[1] = NULL
data[1] = NULL


smpl = data


n = nrow(smpl)
for (i in 1:n) {
  sm = smpl[i,]
  # различаются только временем и/или температурой
  selected = smpl[smpl$trunk_code == sm$trunk_code 
                  & smpl$sampling_date == sm$sampling_date 
                  # & smpl$sampling_time_min == sm$sampling_time_min
                  & smpl$vertical == sm$vertical
                  & smpl$horizon_min == sm$horizon_min,]
  
  nmatch = nrow(selected)
  if (nmatch > 1) {
    cat(paste0('Номер пробы: ',i,' число совпадений - ',nmatch,'\n'))
  }
}


# write.csv(smpl,'samples_ordered.csv')
sm = smpl[333,]
smpl[smpl$trunk_code == sm$trunk_code 
     & smpl$sampling_date == sm$sampling_date 
     # & smpl$sampling_time_min == sm$sampling_time_min
     & smpl$vertical == sm$vertical
     
     & smpl$horizon_min == sm$horizon_min,]

samples = read.csv('sample_summary_dump_2022-09-08.csv')
length(samples$sample_id)
aggregate(sample_id ~ filial, data = samples, FUN = length)