from __future__ import print_function
from __future__ import division
import datetime
import uuid
from datetime import date , timedelta,time
import sys
import re
import random
import string
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from ssl import SSLContext,PROTOCOL_TLSv1




def getDecode():
    str_enc="RmdnQGRhTGFrZTUwMDIwKjMz"
    str_enc2="cGZyY2Fzc2FuZHJhc3ZkdA=="
    # printing the decoded string 
    cred1Dec=str_enc.decode('base64', 'strict')[3:13]
    cred2Dec=str_enc2.decode('base64', 'strict')[3:12]

    return cred1Dec, cred2Dec

#print(getDecode())

def geCassandraConnection(cred):
        ssl_context= SSLContext(PROTOCOL_TLSv1)
        ssl_context.load_cert_chain(\
            certfile='{CERT_FILE}.p7b',\
            keyfile='{CERT_FILE}.key'\
           )
	auth_provider = PlainTextAuthProvider(username=cred[1], password=cred[0])
	cluster = Cluster(
    			['{CASSANDRA_IPS}],
                        ssl_context=ssl_context,
    			load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='thika'),
    			auth_provider=auth_provider,
    		port=9042)

	session = cluster.connect()
	session.set_keyspace('qnique')
        return session

def getPreviousDay(noDays):
       currentDate=datetime.datetime.now().strftime("%Y%m%d")
       filesDate=datetime.datetime.strptime(currentDate, '%Y%m%d')-timedelta(days=int(noDays))
       sdate=str(filesDate).split(' ')
       sd = sdate[0].split('-')
       sds=sd[0]+sd[1]+sd[2]
       return str(sds)

def getRunQuery(nodays):
        session=geCassandraConnection(getDecode())
	
        print("Querying Cassandra child_quiz_name columns " + getPreviousDay(nodays))
	rows = session.execute("SELECT  child_quiz_name  FROM qnique.agent_appraisal_score_cards WHERE signoff_date='"+getPreviousDay(nodays)+"'   ALLOW FILTERING;")
	child_quiz_name=[]
	for result_row in rows:
    	    #print(result_row)
    	    search_str_with_start_num_underscore=re.search('^[0-9]_',re.sub('[^a-z0-9A-Z_]','',result_row.child_quiz_name.replace(' ','_' )))
    	    search_str_with_start_num=re.search('^[0-9]',re.sub('[^a-z0-9A-Z_]','',result_row.child_quiz_name.replace(' ','_' )))
            col_str=re.sub('[^a-z0-9A-Z_]','',result_row.child_quiz_name.replace(' ','_' ))
            if search_str_with_start_num_underscore!=None:
                 child_quiz_name.append(col_str[2:])
            elif search_str_with_start_num!=None:
                 child_quiz_name.append(col_str[1:])
            else:
	         child_quiz_name.append(re.sub('[^a-z0-9A-Z_]','',result_row.child_quiz_name.replace(' ','_' )))
        return child_quiz_name

def getAddColumns(nodays):
    child_quiz_name=getRunQuery(nodays)
    print("Done querying columns from cassandra")
    for columns in child_quiz_name:
       try : 
         sql="ALTER TABLE {TABLE_NAME} ADD "+str(columns)+" decimal;"
         print("Adding Columns " + sql)
   	 session.execute("use qnique")
         session.execute(sql)
       except:
         pass

    print(" Done Adding new columns ")

getAddColumns(1)
