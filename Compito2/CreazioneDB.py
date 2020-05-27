#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 23 16:07:44 2020

@author: chaaralessandro
"""


from random import choices
import csv
with open('/Users/chaaralessandro/Desktop/Events.csv','r') as csv_file:
    csv_reader=csv.reader(csv_file)

    NomeEvento = []
    NazioneEvento = []
    LocalitaEvento=[]
    DataEvento=[]
    ind=[]
    weights=[]
    i=0
    for line in csv_reader:
        if i>0:
            NomeEvento.append(line[0])
            NazioneEvento.append(line[1])
            LocalitaEvento.append(line[2])
            DataEvento.append(line[3])
            weights.append(float(line[4]))
            ind.append(i-1)
            i+=1
        else:
            i+=1
            continue
with open('/Users/chaaralessandro/Desktop/tedx_dataset.csv','r') as watch_next:
     with open('/Users/chaaralessandro/Desktop/tedxxx.csv','w') as new_file:
        writer=csv.writer(new_file,delimiter=',',lineterminator='\n')
        reader=csv.reader(watch_next)
        ii=0
        all=[]
        for row in reader:
            if ii==0: 
                all.append([row[0],'NomeEvento','NazioneEvento','LocalitaEvento','DataEvento'])
                ii+=1
            else:
                l=choices(ind,weights)           
                all.append([row[0][:32],NomeEvento[l[0]],NazioneEvento[l[0]],LocalitaEvento[l[0]],DataEvento[l[0]]])
                
            
        
        writer.writerows(all)