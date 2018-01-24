#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 20:22:53 2018

@author: joe
"""
#from src import TerenceTalks
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

tt = TerenceTalks()

dir(tt)

all_talks = tt.talks
all_words_index = tt.talks_index
one_talk_index = tt.talkIndex(tt.talks.iloc[0])

one_talk_index.columns

Base = declarative_base()

class SingleTalkIndex(Base):
    __tablename__ = 'SingleTalkIndex'
    id = Column(Integer, primary_key = True)
    word = Column(String)
    countsize = Column(Integer)
    length = Column(Integer)
    
    def __repr__(self):
        return "id:{},word:{},countsize:{},length{}".format(self.id,self.word,self.countsize,self.length)
    
sti = SingleTalkIndex(id = 1,word = "foo",countsize = 2000,length = 3)

dir(sti)

check = [x for x in dir(sti) if isinstance(x,Column)]

type(sti.id)

type(sti.word)
type(sti.countsize)


