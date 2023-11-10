setwd('/media/carabus/Transcend/investigation/data_science/bdi/plankton/scripts/2023-03-31')

# проверяем список видов
sp18 = read.csv('output/species_records_2018.csv')
sp19 = read.csv('output/species_records_2019.csv')
sp20 = read.csv('output/species_records_2020.csv')
sp21 = read.csv('output/species_records_2021.csv')
sp22 = read.csv('output/species_records_2022.csv')

colnames(sp18)

sp18s = unique(sp18[is.na(sp18$scientific_name),c('sample_code','backbone_key','taxon_rank','sp_verbatim','sp_backbone')])
sp19s = unique(sp19[is.na(sp19$scientific_name),c('sample_code','backbone_key','taxon_rank','sp_verbatim','sp_backbone')])
sp20s = unique(sp20[is.na(sp20$scientific_name),c('sample_code','backbone_key','taxon_rank','sp_verbatim','sp_backbone')])
sp21s = unique(sp21[is.na(sp21$scientific_name),c('sample_code','backbone_key','taxon_rank','sp_verbatim','sp_backbone')])
sp22s = unique(sp22[is.na(sp22$scientific_name),c('sample_code','backbone_key','taxon_rank','sp_verbatim','sp_backbone')])

spIssues = rbind(sp18s,sp19s,sp20s,sp21s,sp22s)
nrow(spIssues)
spIssues = spIssues[!(spIssues$sp_verbatim %in% c('молодь','копеподиты','наупли','науплии','Nauplii','Nauplii .',
'Nauplii Cal.','Nauplii Cal','Nauplii copepoda','Nauplii Copepoda','Nauplii Cycl.','Nauplii-','Nauplii Cyclopida',
'Nauplii Cyclopidae','Nauplii Temoridae','Eudiaptomidae')),]
nrow(spIssues)

write.csv(spIssues,'sp_checking_samples.csv')

unique(sort(spIssues$sp_verbatim))

spIssuesUniq = unique(spIssues[,c('backbone_key','taxon_rank','sp_backbone','sp_verbatim')])
write.csv(spIssuesUniq,'sp_checking_uniq.csv')
