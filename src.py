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
HOME = os.path.dirname(os.path.abspath(__file__))
import csv

def makedir(name):
    if name[0] == '/':
        dn = os.makedirs(HOME + name )
    else:
        dn = HOME + "/" + name
        
    try:
        os.makedirs(dn)
            
        print("made dir {}".format(dn))
    except FileExistsError:
        print("dir {} already exists".format(dn))
        
        
def checkdirs():
    makedir("output")    
    
OUTPUT = HOME + "/output/"

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
        
        checkdirs()
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
            
            textcount = len(self.join_talks_index)
            
            def divavg(row):
                return (row['countsize'] / row['texts_aggregated'])
            
            self.talks_index['texts_aggregated'] = textcount
            self.talks_index['wcavg'] = self.talks_index.apply(divavg,axis = 1)
            
    def talkList(self, query = text("select * "
                                    "from information_schema.tables "
                                    "where TABLE_SCHEMA = 'talks_index' and TABLE_NAME != 'all_talks'")):
        
#        df= self.queryIndex(query)        
#        return list(df.TABLE_NAME.unique())
        result = list(self.talks_index.tablename.unique())
        
        return result
    
    
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
    
    def getTalkIndex(self,tablename):
        return self.queryIndex("select * from {}".format(tablename))
        
    def talkIndex(self,row):
        print ("talkIndex got arg {}".format(row))
        try:
            tn = row['tablename']            
        except TypeError:
            if self.istalk(row):
                tn = row
        
        query = "select * from {}".format(tn)
        
        return self.queryIndex(query)
        
    @staticmethod
    def savefile(name,df, lod = False):
        """
        lod = list of dicts
        creates a csv from a list of dictionaries
        """
        print ("making {} file".format(name))
        name = OUTPUT + name + '.csv'
        
        
        if lod:
            try:
                lod = df.T.to_dict().values()            
            except:
                lod = df
            keys = list(lod[0].keys())
            with open (name, 'w') as out:
                dict_writer = csv.DictWriter(out,keys)
                dict_writer.writeheader()
                dict_writer.writerows(lod)        
            return True
        else:
            df.to_csv(name)
            return True
        
            
            
    def finddfs(self,return_names = False):
        attributes = [x for x in dir(self) if x[0] != "_"]
        resultdict = {}
        resultlist = []

        for item in attributes:            
            suspect = getattr(self,item)
            if isinstance(suspect, pd.core.frame.DataFrame):
                if return_names:
                    resultlist.append(item)
                else:
                    resultdict[item] = suspect
        
        if return_names:
            return  resultlist
        else:
            return resultdict
        
    def saveall(self):
        dfs = self.finddfs()
        for name,df in dfs.items():
            self.savefile(name,df)
            
if __name__ == "__main__":    
    tt = TerenceTalks()
#    tt.finddfs()
    tt.saveall() 
    
#
#
#check = tt.talks
#type(check)
##import os
#
#
