#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 00:28:05 2018

@author: joe
"""

import os
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy import text


def findfromlist(targetlist,value):
    return [x for x in matchgenerator(targetlist,setmatch(value))]

def matchgenerator(thelist,matchfunc):
    for x in thelist:
        if matchfunc(x):
            yield x        
        
def setmatch(x):        
    def likematch(listitem):
        if x in listitem:
#            print ("x {} is in {} ".format(x,listitem))
            return True
        else:
            if x.upper() in listitem.upper():
                return True
            else:
                return False
#            print ("x {} is not in {} ".format(x,listitem))
    return likematch


def getConnection(cs):
    eng = sqlalchemy.create_engine(cs)
    return eng.connect()

def connectionStringToDf(cs,sql):    
    connection = getConnection(cs)
    return sqlToDF(sql,connection)
    

def sqlToDF(sql,con):
    return resultToDf(con.execute(sql))
    
def resultToDf(results):
    lor = results.fetchall()
    return pd.DataFrame(lor, columns = lor[0].keys())

def loadData():
    talks = "mysql://python:pass@localhost/askterencemckenna"
    talks_index = "mysql://python:pass@localhost/talks_index"
    talks = connectionStringToDf(talks, "select * from talks")
    talks_index = connectionStringToDf(talks_index, "select * from all_talks")
    return talks, talks_index

def getTalk(name):    
    talklist = talkList()
    if name in talklist:
        return 
    


def talkList():
    return list(check.query("TABLE_SCHEMA == 'talks_index'").TABLE_NAME.unique())
    
class TerenceTalks(object):
    def __init__(self,talks_db = "mysql://python:pass@localhost/askterencemckenna"\
                     ,index_db = "mysql://python:pass@localhost/talks_index"\
                     ,talks_query = "select * from talks"
                     ,talks_index_query = "select * from all_talks"):
        
        self.talks_db = talks_db
        self.index_db = index_db
        
        
        self.talks = self.queryTalks(talks_query)
        self.talks_index = self.queryIndex(talks_index_query)
        self.join_talks_index = self.queryTalks(("select * from talks_index_info"))        
        
        self.postprocess()


    def postprocess(self):
            drop_cols = ['audio','location','status','revisions','transcribers','link','youtube','pdf','shortlink']
            self.talks = self.talks.drop(drop_cols,axis = 1).set_index("id")
            
            drop_cols = ['id','title','date','shortlink']
            self.join_talks_index = self.join_talks_index.drop(drop_cols,axis = 1).set_index("talkid")
            
            self.talks = self.talks.merge(self.join_talks_index,left_index = True, right_index = True)        
            
            self.talks_index = self.talks_index.set_index("id")        
        
    def talkList(self, query = text("select * "
                                    "from information_schema.tables "
                                    "where TABLE_SCHEMA = 'talks_index' and TABLE_NAME != 'all_talks'")):
        
        df= self.queryIndex(query)        
        return list(df.TABLE_NAME.unique())
    
    
    def istalk(self,name):
        if name in self.talkList():
            return True
        else:
            return False
        
    @staticmethod
    def _query_something(connection_string, query):
        return connectionStringToDf(connection_string,query)
        
    def queryTalks(self,query):
        return self._query_something(self.talks_db,query)
    
    def queryIndex(self,query):
        return self._query_something(self.index_db,query)
    
    def getTalkIndex(self,talkid):
        tn = self.talks.loc[talkid]["tablename"]
        query = "select * from {}".format(tn)
        self.queryIndex(query)
        
        
        
    

if __name__ == "__main__":
    tt = TerenceTalks()
    
check = tt.talks

#unique_table_names = tt.join_talks_index.tablename.unique()
#
#tt.talks.columns
#
#tt.talks = tt.talks[['id','title','location','talk_text','word_count','review_count']].set_index("")
#
#    
#        
#
#tt.join_talks_index.columns
##
##tt.talks = tt.talks.drop(drop_cols,axis = 1).set_index("id")
##
##check = tt.talks_index
##
##tt.talks_index = tt.talks_index[['id','word','countsize','length']].set_index("id")
##
##tt.jointalks_index = tt.join_talks_index.drop(["id","title","date","shortlink"],axis = 1)\
##                                        .set_index("talkid")
##
##
##check = tt.join_talks_index 
##
##talks = tt.talks.set_index("id")
##
##
##merged = talks.merge(jti, left_index = True, right_index = True)
##
##result = merged[[]]