# started 2022-04-27 версия для SQLite
# текущая версия от 2022-09-05
# перешли на створы
# добавлен буфер названий таксонов с данными из GBIF Backbone


# функция обработки импортируемого списка пунктов сбора перед занесением в базу
# в dataframe с пунктами добавляем номер гидробиологического района по названию 
hbregion <- function(pnts) {
  pnts$num = 0
  pnts$num[pnts[,1] == 'Каспийский'] = 1
  pnts$num[pnts[,1] == 'Черноморский'] = 2
  pnts$num[pnts[,1] == 'Азовский'] = 3
  pnts$num[pnts[,1] == 'Балтийский'] = 4
  pnts$num[pnts[,1] == 'Баренцево-Беломорский'] = 5
  pnts$num[pnts[,1] == 'Карский'] = 6
  pnts$num[pnts[,1] == 'Восточно-Сибирский'] = 7
  pnts$num[pnts[,1] == 'Тихоокеанский'] = 8
  return(pnts)
}


# сохранить таблицу из базы данных в виде отдельного файла формата CSV (таблица)
# используется для выгрузки справочников и результатов импрорта и обработки
selectToCSV <- function(table,d) {
  select = paste0('SELECT * FROM ',table,';')
  result = dbGetQuery(connection, select)
  # вставляем текущую дату в название или нет
  if (d==1) {
    filename = paste0(table,'_dump_',Sys.Date(),'.csv')  
  } else {
    filename = paste0(table,'.csv')
  }
  write.csv(result,filename)
}


# выправляем названия водных объектов и пунктов отбора проб, перед импортом
verbatimClean <- function(objname) {
  objname = sub('пгт.','пгт ',objname)
  objname = sub('вдхр.','вдхр',objname)
  
  # тип объекта вперед
  if (str_detect(objname,'вдхр')) {
    if (!str_starts(objname,'вдхр')) {
      objname = paste0('вдхр ',sub('вдхр','',objname))
    }
  } 
  
  if (str_detect(objname,'пруды')) {
    if (!str_starts(objname,'пруды')) {
      objname = paste0('пруды ',sub('пруды','',objname))
    }
  } 
  
  if (str_detect(objname,'протока')) {
    if (!str_starts(objname,'протока')) {
      objname = paste0('протока ',sub('протока','',objname))
    }
  } 

  if (str_detect(objname,'залив')) {
    if (!str_starts(objname,'залив')) {
      objname = paste0('залив ',sub('залив','',objname))
    }
  } 

  if (str_detect(objname,'губа')) {
    if (!str_starts(objname,'губа')) {
      objname = paste0('губа ',sub('губа','',objname))
    }
  } 
  
  objname = sub(' -','-',objname)
  objname = sub('\\.','. ',objname)
  objname = str_squish(objname)  
}


# СПРАВОЧНИК заполнение таблицы с водными объектами
waterBodiesTable <- function(wTable) {
  # структура таблицы справочника водных объектов
  crtable = 'CREATE TABLE IF NOT EXISTS water_bodies (ids integer PRIMARY KEY AUTOINCREMENT, region smallint,
                                        code integer, wb_name varchar);'
  dbExecute(connection, crtable) # выполнение запроса на создание таблицы

  pnts = read_excel(paste0('dics/',wTable))
  # гидрологические районы
  pnts = hbregion(pnts) # добавляем ноле №13 - num
  
  # гидрологический регион, код водного объекта, название водного объекта, код гидр. региона
  bodies = unique(pnts[,c(1,2,3,14)]) 
  # таблица для проверки
  # write.csv(pnts,'wbcheck.csv')
  
  # задаём названия в dataframe для первых трех полей: гидрологический регион, код водного объекта, название водного объекта
  colnames(bodies)[1] = 'gregion'
  colnames(bodies)[2] = 'code'
  colnames(bodies)[3] = 'name'
  
  # сортируем dataframe по номеру гидробиологического района и названию водного объекта
  bodies = arrange(bodies, num, name)
  n = nrow(bodies)
  
  # для тех объектов, которые не указаны, совпадения не нашлись, в т.ч. название написано неверно
  insert = "INSERT INTO water_bodies VALUES (0,0,0,'not specified');"
  dbExecute(connection,insert)

  # основной цикл загрузки записей
  for (i in 1:n) {
    # i = 256
    wbody = bodies[i,]
    regnum = wbody$num
    wbcode = wbody$code
    wbcode[is.na(wbcode)] = 9999
    wbname = wbody$name
    cat(paste0('№ ',i,'\t'))
    cat(wbname)
    insert = paste0('INSERT INTO water_bodies (region,code,wb_name) VALUES (',regnum,',',wbcode,',\'',wbname,'\');')
    # cat('\n',insert,'\n')
    dbExecute(connection, insert) # запрос на добавление очередной записи в справочник
    # cat('\t в базу добавлено')
    cat('\n')
  }
  cat('DONE\n')
}


# СПРАВОЧНИК импорт гидробиологических пунктов
stantionsToBase <- function(pnts) {

  n = nrow(pnts)

  for (i in 1:n) {
    cat(paste0('№ ',i,'\t'))
    pnt = pnts[i,] # берем очередную запись для импорта
    # cat(paste(pnt,','))
    # cat('\t')
    # сначала проверяем, есть ли уже эта запись в базе по гидробиологическому коду
    # с 2022-07-06 перешли на коды створов (на основе ГХИ) как идентификаторы

    wbodyName = pnt[,3]
    hbcode = pnt[,4]
    hchcode = pnt[,5]
    trunkcode = pnt[,6]
    pointname = pnt[,7]
    lat = round(as.numeric(pnt[,8]),5)  
    lon = round(as.numeric(pnt[,9]),5)
    cat(paste0(pointname,'\t'))
    taddress = pnt[,10]
    tnumber = pnt[,11]
    vertical = pnt[,12]
    # vertical[is.na(vertical)] = 99 # отсутсвие значение заменяем на 99
    
    num = pnt[,14]

    # без кода (гидробиологического) не импортируем, при обновлении
    # if (is.na(hbcode)) {
    #  cat('Без гидробиологического кода\n')
    #  next
    #}
    
    # без кода створа не импортируем
    if (is.na(trunkcode)) {
      cat('Без кода створа - пропускаем\n')
      next
    }
    
    # проверяем, есть ли уже в базе пункт с таким кодом (3 - по ID створа)
    codeCheck = stationID(trunkcode,3)

    # если запись с таким кодом створа уже есть - то её пропускаем и сразу переходим к следующей
    if (codeCheck > 0) {  
      cat('В базе уже есть\n')
      next
    }

    # если какие-то свойства или аттрибуты не указаны - присваиваем значение null, для корректного составления SQL запроса
    if (is.na(hchcode)) {
      hchcode = 'null'
    }

    if (is.na(hbcode)) {
      hbcode = 'null'
      stype = 'null'
    } else {
      stype = substr(as.character(hbcode),2,2)
    }

    if (is.na(pointname)) {
      pointname = 'null'
    } else {
      pointname = paste0('\'',pointname,'\'')
    }

    if (is.na(taddress)) {
      taddress = 'null'
    } else {
      taddress = paste0('\'',taddress,'\'')
    }

    if (is.na(tnumber)) {
      tnumber = 'null'
    }

    if (is.na(lat)) {
      lat = 'null'
    }

    if (is.na(lon)) {
      lon = 'null'
    }

    if (is.na(vertical)) {
      vertical = 'null'
    }

    sql = paste0('SELECT ids FROM water_bodies WHERE region = ',num,' AND wb_name = \'',wbodyName,'\';')
    # cat(paste0('\n',sql,'\n'))
    result = dbGetQuery(connection,sql)
    wbodyID = result[1,1]

    insert = paste0('INSERT INTO hydrobiological_stantions (wbody_id,stantion_type,hb_code,hch_code,trunk_code,stantion_name,latitude,longitude,trunk_address,trunk_number,vertical) VALUES ('
                    ,wbodyID,',',stype,',',hbcode,',',hchcode,',',trunkcode,',',pointname,',',lat,',',lon,',',taddress,',',tnumber,',\'',vertical,'\');')
    # cat(paste0('\n',insert,'\n'))
    dbExecute(connection,insert)
    # cat('в базу добавлено')
    cat('\n')
  }
}


# справочная таблица с гидробиологическими пунктами (только структура с одной записью "не указано")
pointsTable <- function(wTable) {
  # структура таблицы
  crtable = paste0('CREATE TABLE IF NOT EXISTS hydrobiological_stantions ',
                   ' (ids integer PRIMARY KEY AUTOINCREMENT, wbody_id int REFERENCES ',
                   ' water_bodies(ids) ON UPDATE CASCADE ON DELETE CASCADE,',
                   ' stantion_type smallint, hb_code int, hch_code int, trunk_code int,',
                   ' stantion_name varchar, latitude decimal(7,5),',
                   ' longitude decimal(8,5), trunk_address varchar,',
                   ' trunk_number varchar, vertical varchar);')
  dbExecute(connection, crtable)

  # для тех объектов, которые не указаны, совпадения не нашлись, в т.ч. название написано неверно
  insert = paste0("INSERT INTO hydrobiological_stantions (ids,wbody_id,",
                  "stantion_type,hch_code,hb_code,trunk_code,stantion_name) ",
                  "VALUES (0,0,0,0,0,0,'not specified');")
  dbExecute(connection,insert)

  pnts = read_excel(paste0('dics/',wTable))
  # гидрологические районы
  pnts = hbregion(pnts)
  # вносим список в базу
  stantionsToBase(pnts)

  # список гидробиологических станций
  crview = paste0('CREATE VIEW stations AS SELECT hydrobiological_stantions.ids,',
                  'wbody_id,water_bodies.code wb_code,water_bodies.region wb_region,',
                  'water_bodies.wb_name wb_name,hb_code,hch_code,trunk_code,stantion_type,',
                  'stantion_name,latitude,longitude,trunk_address,trunk_number,',
                  'vertical FROM hydrobiological_stantions JOIN water_bodies ',
                  'ON water_bodies.ids = hydrobiological_stantions.wbody_id;')
  dbExecute(connection, crview)
  selectToCSV('stations',1)
  cat('DONE\n')
}


# функция возвращающая ID вида, уже внесенного в базу, по названию (verbatim)
getIDspecies <- function(sp) {
  sql = paste0('SELECT ids FROM species WHERE sp_verbatim = \'',sp,'\';')
  cat(paste0('\n',sql,'\n'))
  result = dbGetQuery(connection,sql)
  ids = result[1,]
  return(ids)
}


# функция добавляющая в таблицу справочника в базе по заданному названию 
# сведения о виде из GBIF Taxonomy Backbone
fromBackbone <- function(ids, sp, genus = NA) {
  
  if (is.na(genus)) {
    result = name_backbone(sp, kingdom = 'Animalia')
  } else {
    result = name_backbone(sp, kingdom = 'Animalia', genus = genus)
  } 
    
  match = result$matchType

  if (match == 'NONE') {
    upd = paste0('UPDATE species SET match = \'',match,'\' WHERE ids = ',ids,';')
  } else {
    name = result$scientificName
    key = result$usageKey
    rank = result$rank
    status = result$status

    kingdom = result$kingdom
    phylum = result$phylum
    clss = result$class
    order = result$order
    family = result$family
    genus = result$genus

    upd = paste0('UPDATE species SET scientific_name = \'',name,
                 '\', backbone_key = ',key,', rank_ = \'',rank,
                 '\', status = \'',status,'\', match = \'',match,'\', kingdom = \'',kingdom,
                 '\', phylum = \'',phylum,'\', class_ = \'',clss,'\', order_ = \'',order,
                 '\', family_ = \'',family,'\', genus = \'',genus,
                 '\' WHERE ids = ',ids,';')
  }
  # cat('\n')
  cat(paste0(match,'\t',rank))
  # cat(paste0('\n',upd,'\n'))
  dbExecute(connection,upd)
}


# функция добавляющая индикатор сапробности в отдельную таблицу
spSaprobity <- function(ids, ind) {
  insert = paste0('INSERT INTO saprobity VALUES(',ids,',',ind,');')
  # cat(paste0('\n',insert,'\n'))
  dbExecute(connection,insert)
}


# справочник с видами
speciesTable <- function(wTable) {

  # процедуру начинаем с создания таблицы
  crsp = paste0('CREATE TABLE IF NOT EXISTS species (ids integer PRIMARY KEY AUTOINCREMENT,',
              # 'sp_verbatim varchar UNIQUE,', 
              'sp_verbatim varchar,',
              'scientific_name varchar,backbone_key integer,accepted_key integer,',
              'rank_ varchar, status varchar, match varchar,',
              'kingdom varchar, phylum  varchar, class_ varchar, order_ varchar,',
              'family_ varchar, genus varchar);')
  dbExecute(connection, crsp)

  # таблица с сапробнотью
  crsapr = paste0('CREATE TABLE IF NOT EXISTS saprobity (species_id int',
                  ' PRIMARY KEY REFERENCES species(ids) ON UPDATE CASCADE ',
                  ' ON DELETE CASCADE, ind_value decimal(3,2) NOT NULL);')
  dbExecute(connection, crsapr)

  sptable = read_excel(paste0('dics/',wTable))
  colnames(sptable)[2] = 'species'
  colnames(sptable)[4] = 'saprobity'
  sptable = arrange(sptable, species) # выстраиваем в алфавитном порядке - по видам

  spugsm = sptable$species
  spsapr = sptable$saprobity
  
  n = length(spugsm)
  cat(paste0('число видов: ',n))
  cat('\n')
  
  # обработки всех названий из таблицы
  for (i in 1:n) {
    cat(paste0("record № ",i,'\t'))
    sp = spugsm[i]
    cat(sp,'\n')

    insert = paste0('INSERT INTO species (sp_verbatim) VALUES (\'',sp,'\');')
    # cat('\n',insert,'\n')
    dbExecute(connection, insert)

    ids = getIDspecies(sp)

    # добавляем данные из GBIF Taxonomy Backbone
    fromBackbone(ids, sp)

    # добавляем сапробность
    sapr = spsapr[i]
    if (!is.na(sapr)) {
      cat(paste0('\nиндикатор сапробности\t',sapr))
      spSaprobity(ids,sapr)
    }
    cat('\n\n')
  }
  
  # справочник с таксонами по BackBone, вместе с сапробностью
  crview = paste0('CREATE VIEW taxons AS SELECT DISTINCT scientific_name, ',
                  'sp_verbatim verbatim_name, ids species_id, backbone_key, rank_, kingdom, ', 
                  'phylum, class_, order_, family_, genus FROM species ',
                  'ORDER BY backbone_key;')
  dbExecute(connection, crview)
  
  # справочник с сапробностью
  crview = paste0('CREATE VIEW sp_saprobity AS SELECT DISTINCT species_id, scientific_name,',
                  'backbone_key,class_, ind_value FROM species, saprobity ',
                  'WHERE species.ids = saprobity.species_id;')
  dbExecute(connection, crview)
  

  # ДОПОЛНЕНИЕ к таблице видов
  # виды с 'sp.', который почему-то до уровня семейства находится или более высокого
  fromBackbone(17,  'Alona dentata', 'Alona')
  # fromBackbone(23,  'Alona sp.', 'Alona')
  fromBackbone(175, 'Cyclops sp.', 'Cyclops') # глюк неизвестной природы
  fromBackbone(229, 'Dicranophorus sp.', 'Dicranophorus') # если род не указать 
  fromBackbone(299, 'Eurycercus serrulatus','Eurycercus')
  fromBackbone(456, 'Mytilina sp.', 'Mytilina') # выдаёт царство
  fromBackbone(551, 'Polyarthra sp.', 'Polyarthra')
  fromBackbone(618, 'Testudinella sp.', 'Testudinella') 

  # library(rgbif)
  # taxon = name_backbone('Cyclops abyssorum')
  # taxon$genus
  # name_backbone('Cyclops', )
  
  # синонимы, которых нет в Backbone
  fromBackbone(561, 'Wolga spinifera (Western, 1894)')
  upd = paste0('UPDATE species SET status = \'SYNONYM\' WHERE ids = 561;')
  dbExecute(connection, upd)
  
  fromBackbone(430, 'Monommata longiseta')
  upd = paste0('UPDATE species SET status = \'SYNONYM\' WHERE ids = 430;')
  dbExecute(connection, upd)
  
  fromBackbone(195, 'Rhynchotalona falcata')
  upd = paste0('UPDATE species SET status = \'SYNONYM\' WHERE ids = 195;')
  dbExecute(connection, upd)
  
  selectToCSV('taxons',1)
  selectToCSV('species',0)
  selectToCSV('sp_saprobity',1)
  
  cat('DONE\n')
}


# справочник с УГМС (филиалами), по уму надо бы из отдельной таблицы грузить
filialsTable <- function() {
  # таблица с УГМС
  crtable = paste0('CREATE TABLE IF NOT EXISTS filials ',
                   '(id smallint PRIMARY KEY, ',
                   'rus_name varchar, ',
                   'lat_name varchar, ',
                   'short_name varchar unique);')
  dbExecute(connection, crtable)

  insert = "INSERT INTO filials VALUES (1,'Северо-Западное УГМС (г. Санкт-Петербург)','Severo-Zapadnoe','SP');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (2,'Псковский ЦМС (г. Псков)','Pskovsky','PS');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (3,'Карельский ЦМС (г. Петрозаводск)','Karelsky','PZ');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (4,'Северное УГМС (г. Архангельск)','Severnoe','AR');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (5,'Иркутское УГМС (г. Иркутск)','Irkutskoe','IR');"
  dbExecute(connection, insert)  
  insert = "INSERT INTO filials VALUES (6,'Среднесибирское УГМС (г. Красноярск)','Sredne_Sibirskoe','KR');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (7,'Дальневосточное УГМС (г. Хабаровск)','Dalnevostochnoe','DV');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (8,'Приморское УГМС (г. Владивосток)','Primorskoe','VL');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (9,'УГМС Республики Татарстан (г. Казань)','Tatarstan','KA');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (10,'Мурманское УГМС (г. Мурманск)','Murmanskoe','MM');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (11,'Верхне-Волжское УГМС (г. Нижний Новгород)','Verkhne_Volgskoe','NN');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (12,'Приволжское УГМС, Тольяттинское СГМО (г. Тольятти)','Privolzhskoe','TT');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (13,'Забайкальское УГМС (г. Чита)','Zabaykalskoe','ZB');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (14,'Тиксинский филиал Якутского УГМС (п. Тикси)','Yakutskoe','TX');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (15,'Северо-Кавказский УГМС (г. Ростов-на-Дону)','Severo_Kavkazskoe','RD');"
  dbExecute(connection, insert)
  insert = "INSERT INTO filials VALUES (16,'Астраханский ЦГМС Северо-Кавказского УГМС (г. Астрахань)','Astrakhan','AS');"
  dbExecute(connection, insert)
  cat('DONE\n')
}


# таблицы со сборами и записями отдельных видов - только структура
rawTables <- function() {
  # ТАБЛИЦЫ СО СБОРАМИ
  crtable = paste0('CREATE TABLE IF NOT EXISTS samples (ids integer PRIMARY KEY AUTOINCREMENT,',
                   'sample_code varchar UNIQUE,',
                   'water_body_id int REFERENCES water_bodies(ids),',
                   'station_id int REFERENCES hydrobiological_stantions(ids) ON UPDATE CASCADE ON DELETE CASCADE,',
                   'filial varchar REFERENCES filials(short_name) ON UPDATE CASCADE ON DELETE CASCADE,',
                   'water_body_verbatim varchar, station_verbatim varchar, ',
                   'sampling_date date, sampling_time_min time, sampling_time_max time,',
                   'sampling_time_verbatim varchar,',
                   # ' trunk smallint,',
                   ' trunk varchar,', # 2022-03-31 поменяли на текстовое поле из-за Мурманска
                   # ' vertical decimal(2,1),',
                   ' vertical varchar,', # 2022-03-31 поменяли на текстовое поле из-за Мурманска
                   'horizon_min decimal(3,1), horizon_max decimal(3,1), ',
                   'horizon_verbatim varchar, ',
                   'temperature decimal(3,1), instrument varchar);')
  dbExecute(connection, crtable)

  # ТАБЛИЦА С найденными видами + обилие
  crtable = paste0('CREATE TABLE IF NOT EXISTS occurrences ',
                   '(ids integer PRIMARY KEY AUTOINCREMENT, ',
                   'sample_id int REFERENCES samples(ids) ON UPDATE CASCADE ON DELETE CASCADE,',
                   'species_id int, ', # насколько виды из таблиц с певичкой будут соответствовать локальному справочнику
                   'backbone_key int, taxon_rank  varchar,',
                  #'species_id int REFERENCES species(backbone_key),', # из BackBone могут вылезать виды, которых нет в справочнике
                   'sp_backbone varchar, sp_verbatim varchar,',
                   ' ex_count decimal(7,0), biomass decimal(10,5),',
                   ' count_m3 decimal(11,2), biomass_m3 decimal(11,5));')
  dbExecute(connection, crtable)

  # Обновление таблицы с видами в процессе работы с данными
  # для отслеженивания, когда добавлено в справочник
  alter = 'ALTER TABLE species ADD COLUMN updated date;'
  dbExecute(connection,alter)
  cat('DONE\n')

  createtable = paste0('CREATE TABLE samples_calculation (ids integer PRIMARY KEY AUTOINCREMENT,',
                       ' sample_id int REFERENCES samples(ids) ON UPDATE CASCADE ON DELETE CASCADE,',  
                       ' shannon decimal(8,7), pielou decimal(8,7));')
  dbExecute(connection, createtable)
}


dropViews <- function(){
  drview = 'DROP VIEW IF EXISTS samples_sum;'
  dbExecute(connection, drview)
  drview = 'DROP VIEW IF EXISTS total_dump;'
  dbExecute(connection, drview)
  drview = 'DROP VIEW IF EXISTS samples_sum;'
  dbExecute(connection, drview)
  drview = 'DROP VIEW IF EXISTS samples_indicator;'
  dbExecute(connection, drview)
  drview = 'DROP VIEW IF EXISTS saprobity_1;'
  dbExecute(connection, drview)
  drview = 'DROP VIEW IF EXISTS species_records_all;'
  dbExecute(connection, drview)
  drview = 'DROP VIEW IF EXISTS saprobity_2;'
  dbExecute(connection, drview)
  drview = 'DROP VIEW IF EXISTS samples_summary;'
  dbExecute(connection, drview)
  cat('Представления (хранимые запросы) удалены')
}


# расчётные таблицы - VIEW
createViews <- function() {
  # большая таблица со всей первичкой
  crview = paste0("CREATE VIEW total_dump AS SELECT samples.ids sample_id, samples.sample_code, occurrences.ids occur_id, ",
                  "CASE region WHEN 1 THEN 'Каспийский' WHEN 2 THEN 'Черноморский' ",
                  "WHEN 3 THEN 'Азовский' WHEN 4 THEN 'Балтийский' ",
                  "WHEN 5 THEN 'Баренцево-Беломорский' WHEN 6 THEN 'Карский' ",
                  "WHEN 7 THEN 'Восточно-Сибирский' WHEN 8 THEN 'Тихоокеанский' END region, ",
                  "filial,water_bodies.code water_body_code,water_bodies.wb_name water_body_name, ",
                  "water_body_verbatim,hb_code station_code_hb,hch_code station_code_hch,",
                  "trunk_code, stantion_name, station_verbatim,sampling_date,",
                  "samples.sampling_time_verbatim,sampling_time_min,sampling_time_max,samples.vertical,",
                  "trunk,horizon_verbatim,horizon_min,horizon_max,temperature,instrument,",
                  "occurrences.backbone_key,occurrences.taxon_rank,scientific_name,",
                  "occurrences.sp_backbone,occurrences.sp_verbatim,occurrences.species_id,",
                  "ex_count,biomass,count_m3,biomass_m3 ",
                  "FROM samples JOIN water_bodies ON water_bodies.ids = samples.water_body_id ",
                  "JOIN hydrobiological_stantions ON hydrobiological_stantions.ids = samples.station_id ",
                  "LEFT JOIN occurrences ON occurrences.sample_id = samples.ids ",
                  "LEFT JOIN taxons ON taxons.backbone_key = occurrences.backbone_key ",
                  "ORDER BY samples.ids, occurrences.ids;")
  dbExecute(connection, crview)
  
  # таблица с характеристиками проб
  crview = paste0('CREATE VIEW samples_sum AS SELECT DISTINCT sample_id, sample_code,',
                  'max(filial) AS filial,max(water_body_code) water_body_code,',
                  'max(water_body_name) AS water_body_name,',
                  'max(water_body_verbatim) AS water_body_verbatim,',
                  'max(station_code_hb) AS station_code_hb,',
                  'max(station_code_hch) AS station_code_hch,',
                  'max(trunk_code) AS trunk_code,',
                  'max(stantion_name) AS stantion_name,',
                  'max(station_verbatim) station_verbatim,',
                  'max(sampling_date) AS sampling_date,',
                  'max(sampling_time_verbatim) AS sampling_time_verbatim,',
                  'max(sampling_time_min) AS sampling_time_min,',
                  'max(vertical) AS vertical,max(trunk) AS trunk,',
                  'max(horizon_verbatim) AS horizon_verbatim,',
                  'max(horizon_min) AS horizon_min,max(horizon_max) AS horizon_max,',
                  'max(temperature) AS temperature, max(instrument) AS instrument,',
                  'count(DISTINCT backbone_key) AS taxon_count,',
                  'sum(ex_count) AS amount_sum, sum(biomass) AS biomass_sum, ',
                  'sum(count_m3) amount_sum_m3, sum(biomass_m3) biomass_sum_m3 ',
                  'FROM total_dump GROUP BY total_dump.sample_id ',
                  'ORDER BY total_dump.sample_id;')
  dbExecute(connection, crview)

  # характеристика проб по числу и количеству индикаторных видов
  crview = paste0('CREATE VIEW samples_indicator AS SELECT occurrences.sample_id,',
                  'sum(biomass_m3) ind_biomass_sum_m3, sum(count_m3) ind_amount_sum_m3,',
                  'samples_sum.biomass_sum_m3, samples_sum.amount_sum_m3,',
                  'round(sum(biomass_m3) / biomass_sum_m3, 4) ind_biomass_ratio,', 
                  'round(sum(count_m3) / amount_sum_m3, 4) ind_count_ratio,', 
                  'count(occurrences.backbone_key) ind_species_num ', 
                  ' FROM occurrences JOIN sp_saprobity ',
                  'ON sp_saprobity.backbone_key = occurrences.backbone_key ',
                  'LEFT JOIN samples_sum ON samples_sum.sample_id = occurrences.sample_id ',
                  'GROUP BY occurrences.sample_id;')
  dbExecute(connection, crview)
  cat('Представление с суммой индикаторных видов создано/n')
  
  # рассчитываем сапробность
  crview = paste0('CREATE VIEW saprobity_1 AS SELECT ids, occurrences.sample_id,',
                  'occurrences.backbone_key,taxon_rank,occurrences.sp_verbatim,',
                  'ind_value,count_m3,biomass_m3,samples_sum.biomass_sum_m3,',
                  'ind_biomass_sum_m3,samples_sum.amount_sum_m3,',
                  'round(count_m3 / CAST(samples_sum.amount_sum_m3 AS REAL) * 100,2) count_m3_percents,',
                  'CASE WHEN round(count_m3 / CAST(samples_sum.amount_sum_m3 AS REAL) * 100,2) BETWEEN 0 AND 0.99 THEN 1 ',
                  'WHEN round(count_m3 / CAST(samples_sum.amount_sum_m3 AS REAL) * 100,2) BETWEEN 1 AND 3.99 THEN 2 ',
                  'WHEN round(count_m3 / CAST(samples_sum.amount_sum_m3 AS REAL) * 100,2) BETWEEN 4 AND 9.99 THEN 3 ',
                  'WHEN round(count_m3 / CAST(samples_sum.amount_sum_m3 AS REAL) * 100,2) BETWEEN 10 AND 19.99 THEN 5 ',
                  'WHEN round(count_m3 / CAST(samples_sum.amount_sum_m3 AS REAL) * 100,2) BETWEEN 20 AND 39.99 THEN 7 ',
                  'WHEN round(count_m3 / CAST(samples_sum.amount_sum_m3 AS REAL) * 100,2) BETWEEN 40 AND 100 THEN 9 ',
                  'END score FROM occurrences ',
                  'JOIN sp_saprobity ON sp_saprobity.backbone_key = occurrences.backbone_key ',
                  'JOIN samples_sum ON samples_sum.sample_id = occurrences.sample_id ',
                  'JOIN samples_indicator ON samples_indicator.sample_id = occurrences.sample_id ',
                  'WHERE taxon_rank IN (\'SPECIES\',\'SUBSPECIES\');')
  dbExecute(connection, crview)

  crview = paste0('CREATE VIEW species_records_all AS SELECT total_dump.sample_id,',
                  'total_dump.sample_code,occur_id,region,total_dump.filial,',
                  'total_dump.water_body_code,total_dump.water_body_name,',
                  'total_dump.water_body_verbatim,total_dump.station_code_hb,',
                  'total_dump.station_code_hch,total_dump.trunk_code,',
                  'total_dump.stantion_name, total_dump.station_verbatim,',
                  'total_dump.sampling_date, total_dump.sampling_time_verbatim, ',
                  'total_dump.sampling_time_min, total_dump.sampling_time_max, ',
                  'total_dump.vertical, total_dump.trunk, total_dump.horizon_verbatim,',
                  'total_dump.horizon_min, total_dump.horizon_max, total_dump.temperature,',
                  'total_dump.instrument, backbone_key, species_id, taxon_rank, scientific_name,',
                  'sp_backbone, sp_verbatim, ex_count, biomass, count_m3, biomass_m3, ',
                  'amount_sum_m3, biomass_sum_m3,',
                  'round(CAST(count_m3 AS real) / amount_sum_m3 * 100,3) count_m3_percent,',
                  'round(CAST(biomass_m3 AS real) / biomass_sum_m3 * 100,3) biomass_m3_percent ',
                  'FROM total_dump JOIN samples_sum ON ',
                  'samples_sum.sample_id = total_dump.sample_id WHERE sp_backbone IS NOT NULL;')
  dbExecute(connection, crview)

  crview = paste0('CREATE VIEW saprobity_2 AS SELECT sample_id, sum(score) score_sum,',
                  'sum(count_m3_percents) percent_sum, round(sum(score * ind_value) / sum(score),3) saprobity,',
                  'sum(round(biomass_m3 / ind_biomass_sum_m3 * ind_value,3)) saprobity_weighted ',
                  'FROM saprobity_1 GROUP BY sample_id ORDER BY sample_id;')
  dbExecute(connection, crview)
  cat('Создали представления сапробности/n')
  
  # индекс Андронниковой: отнощение численности Cladocera (в данном случае отряда Diplostraca)
  # к 
  crview = paste0("CREATE VIEW coef_andr_1 AS SELECT sample_id, order_, ",
                  "CAST( SUM (count_m3) AS REAL) cladocera FROM occurrences ",
                  "JOIN taxons ON taxons.backbone_key = occurrences.backbone_key ",
                  "WHERE order_ = 'Diplostraca' GROUP BY sample_id;")
  dbExecute(connection, crview)

  crview = paste0("CREATE VIEW coef_andr_2 AS SELECT sample_id, class_, ",
                  "CAST( SUM (count_m3) AS REAL) copepoda FROM occurrences ",
                  "JOIN taxons ON taxons.backbone_key = occurrences.backbone_key ",
                  "WHERE class_ = 'Copepoda' GROUP BY sample_id;")
  dbExecute(connection, crview)

  crview = paste0('CREATE VIEW coef_andr_3 AS SELECT coef_andr_1.sample_id, cladocera, copepoda, ',
                  'round(cladocera / copepoda, 4) coef_andr FROM coef_andr_1 JOIN ',
                  'coef_andr_2 ON coef_andr_1.sample_id = coef_andr_2.sample_id ',
                  'WHERE cladocera IS NOT NULL AND copepoda IS NOT NULL;')
  dbExecute(connection, crview)
  
  
  # индекс Мяэметса - отнощение биомассы ракообразных (Crustacea) к биомассе коловраток (Rotifera)
  crview = paste0("CREATE VIEW coef_meaem_1 AS SELECT sample_id, phylum, ",
                  "CAST( SUM (biomass_m3) AS REAL) crustacea FROM occurrences ",
                  "JOIN taxons ON taxons.backbone_key = occurrences.backbone_key ",
                  "WHERE phylum = 'Arthropoda' GROUP BY sample_id;")
  dbExecute(connection, crview)
  
  crview = paste0("CREATE VIEW coef_meaem_2 AS SELECT sample_id, phylum, ",
                  "CAST( SUM (biomass_m3) AS REAL) rotifera FROM occurrences ",
                  "JOIN taxons ON taxons.backbone_key = occurrences.backbone_key ",
                  "WHERE phylum = 'Rotifera' GROUP BY sample_id;")
  dbExecute(connection, crview)
  
  crview = paste0('CREATE VIEW coef_meaem_3 AS SELECT coef_meaem_1.sample_id, ',
                  ' crustacea, rotifera, round(crustacea / rotifera, 4) coef_meaem ',
                  ' FROM coef_meaem_1 JOIN coef_meaem_2 ON coef_meaem_1.sample_id = coef_meaem_2.sample_id ',
                  ' WHERE crustacea IS NOT NULL AND rotifera IS NOT NULL ',
                  ' AND rotifera <> 0 ORDER BY coef_meaem_1.sample_id;')
  dbExecute(connection, crview)
  cat('создали представления коэффициентов/n')

  crview = paste0('CREATE VIEW samples_summary AS SELECT samples_sum.sample_id, samples_sum.sample_code,',
                  'filial, water_body_code, water_body_name, water_body_verbatim,',
                  'station_code_hb, station_code_hch, trunk_code, stantion_name,',
                  'station_verbatim, sampling_date, samples_sum.sampling_time_verbatim,',
                  'vertical, trunk, horizon_verbatim, temperature, instrument, taxon_count,',
                  'ind_species_num,ind_count_ratio*100,ind_biomass_ratio*100,amount_sum,',
                  'biomass_sum,samples_sum.amount_sum_m3,samples_sum.biomass_sum_m3,',
                  'ind_amount_sum_m3,ind_biomass_sum_m3,shannon,pielou,coef_andr,coef_meaem,',
                  'saprobity,saprobity_weighted FROM samples_sum LEFT JOIN ',
                  'samples_calculation ON samples_calculation.sample_id = samples_sum.sample_id ',
                  'LEFT JOIN samples_indicator ON samples_indicator.sample_id = samples_sum.sample_id ',
                  'LEFT JOIN saprobity_2  ON saprobity_2.sample_id  = samples_sum.sample_id ',
                  'LEFT JOIN coef_andr_3  ON coef_andr_3.sample_id  = samples_sum.sample_id ',
                  'LEFT JOIN coef_meaem_3 ON coef_meaem_3.sample_id = samples_sum.sample_id;')
  dbExecute(connection, crview)
}


### ИМПОРТ - ДАННЫЕ В БАЗУ ###
### ПРОЦЕДУРЫ ДЛЯ ПРОВЕРКИ кодов, названий и номера створа
# ID водоёма (по коду) - если такого нет, результат - 0 
waterBodyID <- function(code) {
  # cat('COde: ',code)
  select = paste0('SELECT ids FROM water_bodies WHERE code = ',code,';')
  result = dbGetQuery(connection,select)[1,1]
  # cat('Запрос выполнен\n')
  
  if (!is.na(result)) {
    return(result)
  } else {
    result = 0
    return(result)
  }
}


# ID водоёма по названию
waterBodyIDn <- function(name) {
  # name = 'р. Тёша'
  name = paste0('\'',name,'\'')

  sql = paste0('SELECT ids FROM water_bodies WHERE wb_name = ',name,';')
  result = dbGetQuery(connection,sql)[1,1]
  
  if (!is.na(result)) {
    return(result)
  } else {
    result = 0
    return(result)
  }
}


# IDs гидробиологической станции (ГБ, ГХИ или створ: ГХИ + номер створа) - если нет, результат - 0 
stationID <- function(code,v) {
  
  # сначала проверяем, сколько станций с таким кодом, ожидается, что будет 0 или 1
  if (v==1) { # по гидробиологического коду
    sql = paste0('SELECT count(ids) FROM hydrobiological_stantions WHERE hb_code = ',code,';')
  } else if (v==2) { # по гидрохимическому коду
    sql = paste0('SELECT count(ids) FROM hydrobiological_stantions WHERE hch_code = ',code,';')
  } else if (v==3) { # по идентификатору створа
    sql = paste0('SELECT count(ids) FROM hydrobiological_stantions WHERE trunk_code = ',code,';')
  }
  # cat(paste0('\n',sql,'\n'))
  result = dbGetQuery(connection,sql)[1,1]
  # cat('\nчисло станций по заданному коду ',result,'\n')
  
  # если запись с таким кодом нашли, запрашиваем номер записи в таблице (справочнике)
  if (result == 1) {
    # выбираем, по какому коду искать
    if (v==1) {
      sql = paste0('SELECT ids FROM hydrobiological_stantions WHERE hb_code = ',code,';')
    } else if (v==2) {
      sql = paste0('SELECT ids FROM hydrobiological_stantions WHERE hch_code = ',code,';')
    } else if (v==3) {
      sql = paste0('SELECT ids FROM hydrobiological_stantions WHERE trunk_code = ',code,';')
    }
    # cat('\n',sql,'\n')
    result = dbGetQuery(connection,sql)[1,1]
    if (!is.na(result)) {
      return(result)
    } else {
      result = 0
      return(result)
    } 
  } else {
    result = 0 # если ничего не найдено - возвращаем 0
    return(result)
  }
}


# функция для удаления уже внесенных записей, всех или по указанному филиалу
samplePurge <- function(filial = NA) {
  
  # если филиал (УГМС) не указан - удаляем всё
  dltable = 'DELETE FROM samples_calculation;'
  # cat(dltable,'\n')
  dbExecute(connection, dltable)
  
  # если филиал указан - удаляем записи только для этого филиала
  if (is.na(filial)) {
    dltable = 'DELETE FROM samples;'
  } else {
    dltable = paste0('DELETE FROM samples WHERE filial = \'',filial,'\';')
  }
  # cat(dltable,'\n')
  dbExecute(connection, dltable)
  cat(' DONE')
}


# обработка диапазона значений: времени сбора и горизонта
diapHandle <- function(tDiap,tp) {

  # если NA или null - возвращаем, что есть
  if (is.na(tDiap)) {
    tDiap = 'null'
  }
  
  if (tDiap == 'null') {
    output = c(tDiap,tDiap)
    return(output)
  }
  
  if (tp == 't' && substr(tDiap, 1, 2) == '0.') {
    tDiap = as.numeric(tDiap)
    tDiap = tDiap + 0.0001
    h = trunc(tDiap * 24)
    m = round((tDiap * 24-h)*60)
    if (m < 10) {
      hm = paste0(h,':0',m)
    } else {
      hm = paste0(h,':',m)
    }
    output = c(hm,hm)
    return(output)
  }
  
  # cat(paste0('input values: ',tDiap,'\n'))
  tDiap = as.character(tDiap)
  
  # десятичные запятые заменяем на точки
  tDiap = str_replace(tDiap,',','.')
  
  # если диапазон - разбиваем на две части
  if (gregexpr('-',tDiap)[[1]][1] != -1) {
    output = strsplit(tDiap, '-')[[1]]
  } else {
    # если одно значения - обрабатываем в зависимости от типа
    if (tp == 't') {
      # cat('обрабатываем время \n')
      if (gregexpr('\\.',tDiap)[[1]][1] != -1) {
        sDiap = str_replace(tDiap,'\\.',':')
        # если только один знак для минут - добавляем 0, иначе это превращается в первые минуты
        if (nchar(strsplit(sDiap,':')[[1]][2]) == 1) {
          sDiap = paste0(sDiap,'0')
        }
      } else {
        sDiap = paste0(tDiap,':00')
      }
      output = c(sDiap,sDiap)
    } else {
      output = c(tDiap,tDiap)
    }
  }
  return(output)
}


# есть ли проба уже в базе, запрос по характеристикам пробы: ID пункта, дата, время, номер створа, вертикаль горизонт
sampleExists <- function(sample1) {

  if (sample1[3] == 'null') {
    qtime = ' AND sampling_time_min IS NULL '
  } else {
    qtime = paste0(' AND sampling_time_min = \'',sample1[3],'\'')
  }

  if (is.na(sample1[5]) || sample1[5] == 'null') {
    qvert = ' AND vertical IS NULL '
  } else {
    qvert = paste0(' AND vertical = ', sample1[5])
  }

  if (is.na(sample1[6]) || sample1[6] == 'null') {
    horizon = ' AND horizon_max IS NULL '
  } else {
    horizon = paste0(' AND horizon_max = ', sample1[6])
  }

  select = paste0('SELECT ids FROM samples WHERE station_id = ',sample1[1],
                  ' AND sampling_date = \'',sample1[2],'\'',qtime,
                  ' AND trunk = \'',as.numeric(sample1[4]),'\'',qvert,horizon,';')
  
  # cat(paste0('\n',select,'\n'))
  result = dbGetQuery(connection,select)$ids
  l = length(result)
  
  if (l == 0) {
    return(0) # если пробы нет - возвращаем 0
  } else {
    return(result[1])
  }
}


# код для новой пробы
sampleCode <- function(filial,group,year) {
   select = paste0('SELECT count(ids) samplenum FROM samples WHERE filial = \'',filial,'\';')
   number = as.integer(dbGetQuery(connection,select)$samplenum) + 1
   result = paste0(filial,group,'-',year,'-',number)
   return(result)
}


# функция для внесения данных о сборах в базу
sample2base <- function(sampletable,filial,syear) { 
  
  cat('Заносим данные о пробах\n')
  
  #оставляем только первые 11 полей - все, что относится к пробе (sample event)
  samples = as.data.frame(sampletable[,1:11])
  # записываем для проверки
  # write.csv(samples, paste0('raw/samples_check_',filial,'_',as.integer(Sys.time()),'.csv'))
  
  # вертикаль, которая на самом деле - относительное расстояние от берега (вроде левого)
  # промежуточные значения меняем на понятные
  # cat('Заменяем некорректные значения вертикали\n')
  # colnames(samples)
  samples$Вертикаль[samples$Вертикаль == "0,1; 0,3"] = '0.2'
  
  # удаляем нумерацию полей
  if (samples[1,1] == 1) {
    samples = samples[-1,]
  }
  
  # список проб distinct
  samplesu = as.data.frame(unique(samples))
  # набор полей, включая орудие лова

  # удаляем случайно попавшие пустые записи
  # samplesu = samplesu[!is.na(samplesu$`Название пункта`),]
  samplesu = samplesu[!is.na(samplesu[,4]),]
  
    
  # без даты тоже
  # samplesu = samplesu[!is.na(samplesu$`Дата отбора`),]
  samplesu = samplesu[!is.na(samplesu[,6]),]
  
    
  # записываем временный файл с пробами для проверки
  # write.csv(samplesu, paste0('raw/samples_',filial,'_',as.integer(Sys.time()),'.csv'))
  # cat('Список проб записан в CSV файл \n')

  n = nrow(samplesu)
  
  if (n > 0) {
    cat(paste0('число проб - ',n,'\n'))
  } else {
    cat('В данном листе проб нет - идем дальше \n')
    return()
  }
  
  # ЗАНОСИМ ПРОБЫ В БАЗУ
  
  for (i in 1:n) {
    # i = 1
    wsample = samplesu[i,] # первая запись
    
    # получаем код пробы, состоящий из филиала, группы, года и порядкового номера
    sCode = sampleCode(filial,'Z',syear)

    # получаем идентификатор, который IDs
    stcode = as.integer(wsample[,3])
    if (is.na(stcode) || stcode == '-') {
      stcode = 0
    }
    cat(paste0('Код станции по таблице ',stcode,'\t'))
    # Sys.sleep(3)

    # створ или пункт для озера  !!! обработка значения null
    trunk = as.integer(wsample[,5])
    cat(paste0('створ ',trunk,'\t'))
  
    if (stcode < 100000) {
      trunkcode = stcode * 100 + trunk
    } else {
      # для морских ГХИ код пунтка и станции совпадают
      # trunkcode = stcode
      # cat('\nморской пункт\n')
      # для морских створ трехзначный
      trunkcode = stcode * 1000 + trunk
    }
    cat(paste0('Код створа: ',trunkcode,'\t'))
    
    # перешли на ID створов
    # stid = stationID(stcode,2) # поиск станции по ГХИ
    # if (stid == 0) {
    #  stid = stationID(stcode,1) # поиск станции по ГБ коду 
    # }
    stid = stationID(trunkcode,3)
    cat(paste0('IDs станции (створа) по базе ',stid,'\n'))
    # Sys.sleep(3)
    
    wbcode = wsample[,1]
    # cat('Код водного объекта из таблицы',wbcode,'\n')
    
    # если код водоема не указан - определяем по станции
    # или если вместо кода водоёма указан код станции  
    cat(paste0('Код водного объекта: ',wbcode,' код станции: ',stcode,'\n'))
    # cat(paste0('Длина кода водного объекта: ',nchar(wbcode),' длина код станции: ',nchar(stcode),'\n'))
    if (is.na(wbcode) || wbcode == stcode || wbcode == '-') {
      # cat('хрень какая-то \n')
      select = paste0('SELECT wb_code FROM stations WHERE hch_code = ',stcode,' LIMIT 1;')
      result = dbGetQuery(connection,select)
      wbcode = result[,1]
      if (length(wbcode) == 0) {
        wbcode = 0
      }
    } 
    
    cat(paste0('Код водного объекта ',wbcode,'\t'))
    wbid = waterBodyID(wbcode)
    cat(paste0('IDs водного объекта ',wbid,'\n'))
    
    cat(paste0('Код водного объекта: ',wbcode,' код станции: ',stcode,'\n'))
    if (wbid == 0) {
      # wbname = 'р. Тёша'
      wbname = wsample[,2]
      wbname = verbatimClean(wbname) # форматируем название
      wbid = waterBodyIDn(wbname)
    }
    
    # название пункта
    stname = verbatimClean(wsample[,4])

    # дата
    tDate = wsample[,6]
    
    # Это для Мурманской области - добавляем 15е число
    cat(paste0('Проверяем дату, что она полная: ',tDate,'\n'))
    if (nchar(tDate) == 7) {
      tDate = paste0('15.',tDate)
    }
      
    sDate = convert_to_date(tDate, character_fun = lubridate::dmy)
    
    
    # заносим в таблицу три значения времени отбора пробы: "вербатим", минимальное и максимальное
    # cat(paste0('Время по таблице ',wsample[,7],'\n'))
    timeVerbatim = wsample[,7]
    sTime = diapHandle(timeVerbatim,'t')
    # cat(paste0('Временной диапазон ',sTime[1],' ',sTime[2],'\n'))
    
    if(sTime[1] != 'null') {
      sTime1 = paste0("'",sTime[1],"'")
      sTime2 = paste0("'",sTime[2],"'")
    } else {
      sTime1 = sTime[1]
      sTime2 = sTime[2]   
    }
    
    # cat(paste0('\n функция ',sTime1,'\n'))
    
    vert = str_squish(wsample[,8])
    # cat(paste0('Вертикаль: ',vert,' почему получаем null? \n'))
    if (is.na(vert)) {
      vert = 'null'
    } else if (vert == '-') {
      vert = 'null'
    } else {
      vert = paste0("'",vert,"'")
    }
    
    # заносим в таблицу три значения гозизонта (глубины): "вербатим", минимальное и максимальное
    horizonVerbatim = wsample[,9] 
    hor = diapHandle(horizonVerbatim,'h')
    
    temperature = wsample[,10]
    if (is.na(temperature)) temperature = 'null'
    
    instrument  = wsample[,11]
    
    sampleNew = list(stid,as.character(sDate),sTime[1],trunk,vert,hor[1])
    
    cat(paste(sampleNew))
    cat('\n')
    # Sys.sleep(3)
    
    if (sampleExists(sampleNew) == 0) {
      cat('такой пробы в базе нет - заносим ')
      
      insert = paste0('INSERT INTO samples (sample_code, water_body_id,station_id,water_body_verbatim,',
                      'station_verbatim,filial,sampling_date,sampling_time_min,',
                      'sampling_time_max,sampling_time_verbatim,trunk,vertical,horizon_max,horizon_min,',
                      'horizon_verbatim,temperature,instrument) ',
                      'VALUES (\'',sCode,'\',',wbid,',',stid,',\'',wsample[,2],'\',\'',
                      stname,'\',\'',filial,'\',\'',sDate,'\',',sTime1,
                      ',',sTime2,',\'',timeVerbatim,'\',\'',trunk,'\',',vert,',',hor[1],',',
                      hor[2],',\'',horizonVerbatim,'\',',temperature,',\'',instrument,'\');')
      # cat(paste0('\n',sTime1,'\n'))
      # cat(paste0('\n',insert,'\n'))
      # Sys.sleep(5)
      dbExecute(connection, insert)
      cat(' DONE\n')
    } else {
      cat('проба в базе уже есть - идём дальше \n')
    }
  }
}  


# правим названия видов
speciesTreatment <- function(sp) {
  sp = sub('Ficher','Fischer',sp)
  sp = sub('самец','',sp)
  sp = sub('самка','',sp)
  sp = sub(',',', ',sp)
  sp = sub('\\).',')',sp)
  sp = sub(', \\(',' (',sp)
  sp = sub('\\)',') ',sp)
  #sp = sub(', \\(',' (',sp)
  #sp = sub(',\\(',' (',sp)
  sp = sub('\\(',' (',sp)
  sp = sub('bdelloida','Bdelloidea',sp)
  sp = sub('s-','s',sp)
  sp = sub('Leyding','Leydig',sp)
  sp = sub('Bytotrephes','Bythotrephes',sp)
  # sp = sub('.','. ',sp)
  sp = sub('самк','',sp)
  # sp = sub('diaptomus','Diaptomus',sp)
  sp = sub('♀','',sp)
  sp = sub('♂','',sp)
  # sp = sub('коп.','',sp)
  # sp = sub('науп.','',sp)
  # sp = sub('зр.','',sp)
  sp = sub('\\(M.)','',sp)
  sp = sub('С','C',sp)
  sp = sub('\\(T.)','',sp)
  sp = sub('\\(D.)','',sp)
  # sp = sub('O. F.','O.F.',sp)
  # sp = sub('(O.F.M)','(O.F. Müller, 1776)',sp)
  # sp = sub('ta O.F.Mueller','ta (O.F. Müller, 1776)',sp)
  # sp = sub('na O.F. Müller, 1776','na (O.F. Müller, 1776)',sp)
  sp = sub('Muller','Müller',sp)
  sp = sub('M\\?ller','Müller',sp)
  sp = sub('Miiller','Müller',sp)
  sp = sub('Mueller','Müller',sp)
  sp = sub('O. F.','O.F. ',sp)
  sp = sub('O.F.','O.F. ',sp)
  sp = sub('M.uller','Müller',sp)
  sp = sub('Schoedler','Schödler',sp)
  sp = sub('Carlin 1943','Carlin, 1943',sp)
  sp = sub('Polyarthra vulgaris Carlin, 1843','Polyarthra vulgaris Carlin, 1943',sp)
  sp = sub('Polyarthra vulgaris Carlin, 1943','Polyarthra vulgaris (Carlin, 1943)',sp)
  sp = sub('Polyarthra luminosa Kutikova, 1862','Polyarthra luminosa Kutikova, 1962',sp)
  sp = sub('Car1in','Сarlin',sp)
  sp = sub('Notholca acuminata (Ehr\\.)','Notholca acuminata (Ehrenberg, 1832)',sp)
  # sp = sub('Acanthocyclops vernalis Fisch','Acanthocyclops vernalis (Fischer, 1853)',sp)
  # sp = sub('Acanthocyclops vernalis Fischer','Acanthocyclops vernalis (Fischer, 1853)',sp)
  sp = sub('Acanthocyclops vernalis Fischer, 1863','Acanthocyclops vernalis (Fischer, 1853)',sp)
  sp = sub('Corniger lacustris Spandl','Corniger lacustris (Spandl, 1923)',sp)
  sp = sub('Notolca','Notholca',sp)
  sp = sub('Thysanoessa intermis','Thysanoessa inermis',sp)
  sp = sub('Trichotria similis Stenroos, 1898','Trichotria tetractis subsp. similis (Stenroos, 1830)',sp)
  sp = sub('Bythothrephes','Bythotrephes',sp)
  sp = sub('Harpacticoidae','Harpacticoida',sp)
  sp = sub('Peracanta','Peracantha',sp)
  sp = sub('Polyarthra d.dolichoptera','Polyarthra dolichoptera',sp)
  sp = sub('Ecyclops','Eucyclops',sp)
  sp = sub('De Guerne','de Guerne',sp)
  sp = sub('Brach cal.','Brachionus calyciflorus',sp)
  sp = sub('Brach. cal.','Brachionus calyciflorus',sp)
  sp = sub('Brachion.','Brachionus',sp)
  sp = sub('Brachion. cal.','Brachionus calyciflorus',sp)
  
  
  # sp = sub('','',sp)
  sp = str_squish(sp)
}

# getSpID('Keratella cochlearis (Gosse, 1851)','Keratella cochlearis (Gosse, 1851)')
# getSpID('Microcyclops bicolor (Sars, 1863)','Microcyclops bicolor (Sars, 1863)')

# получаем ID по локальному справочнику
getSpID <- function(spVerbatim,spBackBone) {
  select = paste0('SELECT ids FROM species WHERE sp_verbatim = \'',spVerbatim,'\';')
  # select = "SELECT ids FROM species WHERE sp_verbatim = 'хрен'";
  result = dbGetQuery(connection,select)
  ids = result[,1]
  if (length(ids) == 0) { # если по вербатим не найдено, проверяем по названию из backBone
    select = paste0('SELECT ids FROM species WHERE scientific_name = \'',spBackBone,'\';')
    result = dbGetQuery(connection,select)
    ids = result[,1]    
    if (length(ids) == 0) {
      ids = 0
    } else {
      ids = -ids
    }
  }
  if (length(ids) > 1) ids = ids[1]
  return(ids)
}


# функция для внесения записей в базу
occur2base <- function(sampletable,filial) {
  
  # берем нужные поля из таблицы
  occurs = as.data.frame(sampletable[,c(3,5,6,7,8,9,12,13,14,15,16,17)]) 
  # cat(paste0('Число записей черновое - ',nrow(occurs),'\n'))
  
  # удаляем нумерацию полей
  if (occurs[1,1] == 3) {
    occurs = occurs[-1,]
  }
  
  # удаляем пустые строки
  # occurs = occurs[!is.na(occurs$`Дата отбора`),]
  occurs = occurs[!is.na(occurs[,6]),]
  # cat(paste0('Число записей после удаления пустых строк - ',nrow(occurs),'\n'))  
  
  # удаляем записи не относящиеся к каким-либо таксонам
  occurs = occurs[occurs[,7] != 'Σ',]
  occurs = occurs[occurs[,7] != '∑',]
  occurs = occurs[occurs[,7] != 'все',]
  occurs = occurs[occurs[,7] != 'Все',]
  occurs = occurs[occurs[,7] != 'Организмов не обнаружено',]
  occurs = occurs[occurs[,7] != 'нет организмов',]
  occurs = occurs[occurs[,7] != 'Нет организмов',]
  occurs = occurs[occurs[,7] != 'СУММА',]
  occurs = occurs[occurs[,7] != 'гидробионты отсутствуют',]
  occurs = occurs[occurs[,7] != 'Отбор не осуществлен',]
  occurs = occurs[occurs[,7] != 'нет отбора в июле',]
  
  # occurs = occurs[!is.na(occurs$`Вид`),] # где вид просто не указан
  occurs = occurs[!is.na(occurs[,7]),] # где вид просто не указан
  
  # здесь тоже исправляем
  occurs$Вертикаль[occurs$Вертикаль == "0,1; 0,3"] = '0.2'
  
  # write.csv(occurs, 'raw/occurs_temp.csv')
  occurcount = nrow(occurs)
  cat(paste0('число записей с таксонами - ',occurcount,'\n'))
  if (occurcount == 0) {
    cat('записей нет, переходим к следующему водоёму \n')
    return()
  }

  # для мурманской области
  # cat(paste0('Проверяем дату, что она полная: ',occurs[1,3],'\n'))
  if (nchar(occurs[1,3]) == 7) {
    occurs[,3] = paste0('15.',occurs[,3])
  }
  
  occurs[,3] = convert_to_date(occurs[,3], character_fun = lubridate::dmy)
  
  # загружаем список видов из BackBone
  if (file.exists('BackBoneBuffer.csv')) {
    bblist = read.csv('BackBoneBuffer.csv')
    bblist[,1] = NULL
    cat('BackBone cache загружен\n')
  } else {
    bblist = data.frame(backbone_key = 1, taxon_rank = 'KINGDOM', sp_verbatim = 'Animalia', sp_backbone = 'Animalia')
    cat('dataFrame для BackBone cache создан\n')
  }
  # Sys.sleep(0.3)

  # вносим в таблицу
  n = nrow(occurs)
  
  for (i in 1:n) {
    # i = 1
    occ = occurs[i,]
    cat(paste(occ))
    stcode = as.integer(occ[,1])
    if (is.na(stcode) || stcode == '-') {
      stcode = 0
    }
    trunk = as.integer(occ[,2])
    if (stcode < 100000) {
      trunkcode = stcode * 100 + trunk
    } else {
      # trunkCode = stCode
      # для морских створ трехзначный
      trunkcode = stcode * 1000 + trunk
    }

    # идентификатор пункта - по створу
    stID = stationID(trunkcode,3)
    date = occ[,3] # дата
    sTime = diapHandle(occ[,4],'t') # время
    vert = str_squish(occ[,5]) # вертикаль
    hor = occ[,6] # горизонталь
    # cat(paste0('горизонталь из таблицы: ',hor,'\n'))
    hor = diapHandle(hor,'h')
    # cat(paste0('горизонталь после обработки: ',hor,'\n'))
    
    # vert = as.numeric(vert) # 2022-03-31 выключил, так как текстовые значения попадаются
    # cat(paste0('Вертикаль из таблицы: ',vert,' длина значения: ',length(vert),'\n'))
    if (is.na(vert)) {
      vert == 'null'
    } else if (vert == '-') {
      vert = 'null'
    } else {
      vert = paste0("'",vert,"'")
    }
    
    # cat(paste0('Вертикаль после проверки на NA: ',vert,'\n'))
    sampleQ = list(stID,as.character(date),sTime[1],trunk,vert,hor[1])
    # cat(paste(sampleQ))
    # cat('\n')
    
    sampleID = sampleExists(sampleQ)
    # cat(paste0('Идентификатор пробы: ',sampleID,'\n'))
    
    # таксон - очередная завить в пробе
    spVerb = str_squish(occ[,7]) # удаляем лишние пробелы
    # рихтуем названия
    spVerb = speciesTreatment(spVerb)
    
    sp = spVerb 
    
    if (sp == 'Отсутствует орудие лова') next # это пустая проба - пропускаем
    
    # заменяем некоторые названия
    # sp[sp == 'наупли']  = 'Hexanauplia'  
    # sp[sp == 'науплии'] = 'Hexanauplia'  
    # sp[sp == 'Nauplii'] = 'Hexanauplia'

    sp[sp == 'Nauplii Cyclopidae'] = 'Cyclopidae'
    sp[sp == 'Cop. Cyclopidae'] = 'Cyclopidae'
    
    sp[sp == 'Bdelloidae sp.'] = 'Bdelloidea' # 2023-11-13
    sp[sp == 'Bosminopsis deitersiRichard, 1895'] = 'Bosminopsis deitersi Richard, 1895'
    
    sp[sp == 'Nauplii Temoridae'] = 'Temoridae'
    
    # sp[sp == 'Bosmina longispina Leydig'] = 'Bosmina longispina'
    # sp[sp == ''] = ''

    # name_backbone('Bosmina longispina Leydig', kingdom = 'Animalia')
    # name_backbone('Synchaeta oblonga Ehrenberg', kingdom = 'Animalia')

    sp[sp == 'копеподиты'] = 'Copepoda' # key 9421786  Copepoda - это подкласс, промежуточных таксонов в GBIF нет
    sp[sp == 'Copepoditi'] = 'Copepoda' # key 9421786  Copepoda теперь класс в GBIF https://www.gbif.org/species/11545536
    sp[sp == 'Copepodita'] = 'Copepoda' # key 9421786  Hexanauplia исключили 
    sp[sp == 'Copepoda nauplii I-VI'] = 'Copepoda'
    sp[sp == 'Nauplii copepoda'] = 'Copepoda'
    sp[sp == 'Nayplii Copepoda'] = 'Copepoda'
    sp[sp == 'Copepoda sp.'] = 'Copepoda'
    sp[sp == 'Copepoda'] = 'Copepoda'
    sp[sp == 'Kopepodit'] = 'Copepoda'
    sp[sp == 'Nauplii'] = 'Copepoda'
    sp[sp == 'Nauplii-'] = 'Copepoda'
    sp[sp == 'Kopepodit-'] = 'Copepoda'
    sp[sp == 'Copepoda juv.'] = 'Copepoda'
    sp[sp == 'Copepodita Calanoida'] = 'Calanoida' # key 679 order
    sp[sp == 'Nauplii Calanoidae'] = 'Calanoida'
    sp[sp == 'Copepodita Calanoida'] = 'Cop. Calanoida' # key 679 order
    sp[sp == 'Nauplii Eudiaptomidae'] = 'Diaptomidae'
    sp[sp == 'Notholka sp.'] = 'Notholca sp.' # коловратка
    sp[sp == 'Cop. Heterocopae'] = 'Heterocope'
    sp[sp == 'Harpacticoidae sp'] = 'Harpacticoida'
    sp[sp == 'Copepodita Cyclopoida'] = 'Cyclopoida' # key 687 order
    sp[sp == 'Copepodit Calanoida'] = 'Calanoida' # key 679 order
    sp[sp == 'Copepodit Cyclopoida'] = 'Cyclopoida' # key 687 order
    sp[sp == 'Nauplii Temoridae'] = 'Temoridae'
    sp[sp == 'Acantocyclops sp.'] = 'Acanthocyclops'
    sp[sp == 'Brach cal. calyciflorus'] = 'Brachionus calyciflorus subsp. calyciflorus Pallas, 1766'
    sp[sp == 'Brachion. cal. spinosus'] = 'Brachionus calyciflorus spinosus' # not in Backbone
    sp[sp == 'Cirripedia sp.'] = 'Ciripedium'
    sp[sp == 'Polychaeta larva'] = 'Polychaeta'
    sp[sp == 'Cop. Eurytemora'] = 'Eurytemora'
    sp[sp == 'Onceae borealis'] = 'Triconia borealis' # 4322171
    sp[sp == 'Keratella quadratа (Müller, 1786)'] = 'Keratella quadrata (Müller, 1786)'
    sp[sp == 'Cyclops strenuus Fisch'] = 'Cyclops strenuus Sars G.O., 1903'
    sp[sp == 'Cyclops strenuus Fischer, 1851'] = 'Cyclops strenuus Sars G.O., 1903'
    sp[sp == 'Polyarthra sp.-'] = 'Polyarthra'
    
    
    # вид проверяется по GBIF Taxonomy Backbone, если таксон совсен не найден, то будет код - 1
    group = str_squish(occ[,8]) # группа - если вид не будет найден
    
    group[group == 'Cladocera'] = 'Diplostraca' # бывш. Cladocera # отряд
    group[group == 'Rotatoria'] = 'Rotifera' # тип
    group[group == 'Коловратки'] = 'Rotifera' # тип
    group[group == 'Ветвистоус.'] = 'Diplostraca'
    group[group == 'Веслоногие'] = 'Copepoda' # класс Hexanauplia исключен, теперь Copepoda
    group[group == 'Каляноиды'] = 'Calanoida' # класс 
    
    
    # cat(paste0('\nВид очередной записи ',sp,'\n'))
    # Sys.sleep(5)

    # сначала ищем в локальном списке
    speciesbb = bblist[bblist$sp_verbatim == sp, ]

    if (nrow(speciesbb) != 0) {
      spKey = speciesbb$backbone_key
      rank  = speciesbb$taxon_rank
      spbb  = speciesbb$sp_backbone
      cat('\t Вид найден в локальной таблице')
      cat(sp,'\t',spKey,'\t',rank,'\t',spbb)
      cat('\fFound in GBIF Backbone cache\f')
    } else {
      cat('\nin the GBIF Backbone cache not found\n')
      result = name_backbone(sp, kingdom = 'Animalia')
      spKey = result$usageKey
      rank = result$rank
      spbb = result$scientificName
      # cat(paste0('название по BackBone: ',spbb,'\n'))
      # если вид не найден - берем группу
      if (spKey == 1 ) {
        cat(paste0('Вид в BackBone не найден - берем группу: ',group))
        # Sys.sleep(3)
        if (!is.na(group)) {
          speciesbb = bblist[bblist$sp_verbatim == sp, ]
          if (nrow(speciesbb) != 0) { 
            spKey = speciesbb$backbone_key
            rank  = speciesbb$taxon_rank
            spbb  = speciesbb$sp_backbone
          } else {
            result = name_backbone(group, kingdom = 'Animalia')
            spKey = result$usageKey
            rank = result$rank
            spbb = result$scientificName
            # cat('хрень')
            # cat(paste0(data.frame(backbone_key=spKey,taxon_rank=rank,sp_verbatim=sp,sp_backbone=spbb)))
            # cat('\n')
            bblist = rbind(bblist, data.frame(backbone_key=spKey,taxon_rank=rank,sp_verbatim=sp,sp_backbone=spbb))
          }
        } 
      } else {
        bblist = rbind(bblist, data.frame(backbone_key=spKey,taxon_rank=rank,sp_verbatim=sp,sp_backbone=spbb))
      }
      write.csv(bblist,'BackBoneBuffer.csv')
    }
    
    # ID вида по локальному справочнику
    spID = getSpID(sp,spbb)
    
    
    exCount = occ[,9]   # число экземпляров
    biomass = occ[,10]  # биомасса
    countm3 = occ[,11]  # численность на м3
    biomassm3 = occ[,12] # биомасса на кубометр
 
    # значения NA обилия меняем на null
    if (is.na(exCount)) {
      exCount = 'null'
    }
    if (is.na(countm3)) {
      countm3 = 'null'
    } else {
      # cat(countm3)
      # Sys.sleep(1)
      countm3 = round(as.numeric(countm3),4)
    }
    if (is.na(biomass)) {
      biomass = 'null'
    }
    if (is.na(biomassm3)) {
      biomassm3 = 'null'
    } else {
      biomassm3 = round(as.numeric(biomassm3),4)
    }
 
    # cat('очередная запись пробы \t')
    # cat(sampleID,' ',spVerb,'\t\t',count,'\t',biomass,'\t')
    insert = paste0('INSERT INTO occurrences (sample_id, species_id, backbone_key, taxon_rank,',
                    'sp_backbone, sp_verbatim, ex_count, biomass, count_m3, biomass_m3) ',
                    ' VALUES (',sampleID,',',spID,',',spKey,',\'',rank,'\',\'',spbb,'\',\'',
                    spVerb,'\',',exCount,',',biomass,',',countm3,',',biomassm3,');')
    # cat(paste0('\n',insert,'\n'))
    dbExecute(connection, insert)
    cat(' DONE\n')
  }
  bblist = bblist[order(bblist$sp_verbatim),]
  write.csv(bblist,'bblist.csv')
}


# процедура импорта таблицы с первичными данными
importRawTable <- function(rawFile) {

  # сокращение УГМС

  filial = strsplit(rawFile, split = '_')[[1]][1]
  syear  = substr(strsplit(basename(rawFile), split = '_')[[1]][3],1,4)
  cat(paste0('импортирумый файл: ',rawFile,' филиал: ',filial,' год: ',syear,'\n'))
  
  sheets = excel_sheets(paste0('raw/',rawFile)) # список листов таблицы excel
  sheetcount = length(sheets) # число вкладок (листов) в файле
  
  for (i in 1:sheetcount) {
    
    cat(paste0('Импорт листа ',sheets[i],'\n'))
    # Sys.sleep(5)
    sampletable = read_excel(paste0('raw/',rawFile), sheet = sheets[i]) # берем лист 1
    
    sample2base(sampletable,filial,syear) # вносим пробы в базу с очередного листа 
    occur2base(sampletable,filial)  # вносим записи в базу с очередного листа
    cat('\n')
  }
  cat('Выгрузка таблицы со всеми записями по видам\n')
  selectToCSV('species_records_all',1)
  cat('Импорт таблицы очередного УГМС закончен\n')
}


# корректируем значения численность и биомассы
# для некоторых филиалов значения надо перемножить на 1000
valueAdjust <- function(filial, var) {

  # select samples 
  sql = paste0('SELECT ids FROM samples WHERE filial = \'',filial,'\';')
  result = dbGetQuery(connection, sql)
  
  # class(result[,1])

  if (var == 'count') upd0 = 'UPDATE occurrences SET count_m3 = count_m3 * 1000 WHERE sample_id IN ('
  if (var == 'biomass') upd0 = 'UPDATE occurrences SET biomass_m3 = biomass_m3 * 1000 WHERE sample_id IN ('
  
  upd = paste0(upd0,paste0(result[,1],',',collapse = ""),');')
  upd = sub(',);',');',upd)
  
  dbExecute(connection, upd)
  
}


# расчёт индексов - Шеннон и прочие
indices <- function() {

  sql = paste0("SELECT sample_id, backbone_key, taxon_rank, scientific_name,",
               "sp_verbatim, count_m3 FROM total_dump WHERE ",
               "taxon_rank IN ('SPECIES','SUBSPECIES') AND count_m3 IS NOT NULL;") 
  # на самом деле, если хоть одно значение NULL обилия вида-подвида есть, то всю пробу из расчётов надо исключать
  # а еще лучше - на этапе импорта из первички проверку на отсутсвие NULL значений в пробе надо сделать
  
  raw = dbGetQuery(connection, sql)
  raw2 = raw[, c("sample_id","backbone_key","count_m3")] 
  cross = cast(raw, sample_id ~ backbone_key, sum)

  cross2 = cross
  cross2[1] = NULL
  
  shannon = diversity(cross2, index="shannon")
  pielou  = diversity(cross2) / log(specnumber(cross2))
  
  cross$shannon = shannon
  cross$pielou  = pielou

  # заносим результаты расчёта Шеннона в базу
  dltable = 'DELETE FROM samples_calculation;' # сначала таблицу очищаем
  dbExecute(connection, dltable)
  
  n = nrow(cross)
  for (i in 1:n) {
    id = cross[i,]$sample_id
    sh = round(cross[i,]$shannon,4)
    pl = round(cross[i,]$pielou,4)
    if (is.na(pl)) {
      pl = 'null'
    }

    cat(paste0('проба: ',id,' Шеннон: ',sh,'\tПилау: ',pl,'\n'))
    insert = paste0('INSERT INTO samples_calculation (sample_id, shannon, pielou) ',
                    ' VALUES (',id,',',sh,',',pl,');')
    # cat(insert,'\n')
    dbExecute(connection, insert)
  }
  # выгрузка сводной таблицы с пробами
  selectToCSV('samples_summary',1)
  cat('DONE\n')
}

verbatimDistinct <- function() {
  sql = 'SELECT DISTINCT sp_verbatim FROM occurrences ORDER BY sp_verbatim;'
  result = dbGetQuery(connection, sql)
  write.csv(result,paste0('spVerbatim_',as.integer(Sys.time()),'.csv'))
}