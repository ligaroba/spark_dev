from __future__ import print_function
from __future__ import division
from pyspark.sql import SparkSession
import datetime
import uuid
from datetime import date , timedelta,time
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
import random
import string


'''
Defination of Global Variables 
'''
keyspace="{KEYSPACE_NAME}"
tablename="{DEST_TABLENAME}"

'''
End of Definations of Variables 
'''


def gethashedCredentials():
    str_enc="RmdnQGRhTGFrZTUwMDIwKjMz"
    str_enc2="cGZyY2Fzc2FuZHJhc3ZkdA=="
    cred1Dec=str_enc.decode('base64', 'strict')[3:13]
    cred2Dec=str_enc2.decode('base64', 'strict')[3:12] 
    return cred1Dec, cred2Dec


def getSequence():
    seq=random.randint(1,100)
    return seq
     #
     #.config("spark.cassandra.connection.host", 'LIST OF IPS')\
def getSparkSession():
    if ("sparksession" is not globals()):
            globals()["sparksession"] = SparkSession \
                .builder \
                .appName("Qnique Score card appraisal") \
                .config("spark.cassandra.connection.host", "{CASSNDRA_IPS}")\
                .config("spark.cassandra.auth.password", gethashedCredentials()[0])\
                .config("spark.cassandra.auth.username", gethashedCredentials()[1])\
                .config("spark.cassandra.connection.ssl.clientAuth.enabled","true")\
                .config("spark.cassandra.connection.ssl.keyStore.password","{KEYSTORE_PASS}")\
                .config("spark.cassandra.connection.ssl.trustStore.password","{TRUSTSTORE_PASS}")\
                .config("spark.cassandra.connection.ssl.keyStore.path","{PATH_TO_KEYSTORE}")\
                .config("spark.cassandra.connection.ssl.trustStore.path","{PATH_TO_TRUSTORE}")\
                .config("spark.cassandra.connection.ssl.enabled","true")\
                .getOrCreate()
    return globals()["sparksession"]
	
	
	
def getWriteTable(df,tablename,keysp):
      #df.map(tuple).saveToCassandra(tablename,keysp)
       df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=tablename, keyspace=keysp)\
        .save()


		
def getReadQniqueCassandra(sql):
        getSparkSession().catalog.dropTempView("uvc_match_sales")
        createDDL = """CREATE TEMPORARY VIEW {VIEW_NAME}\
                 USING org.apache.spark.sql.cassandra\
                 OPTIONS (\
                 table "{TABLE_NAME}",\
                 keyspace "{KEYSPACE}",\
                 cluster "{CASSANDRA_CLUSTER_NAME}",\
                 pushdown "true")"""
        getSparkSession().sql(createDDL)
	df=getSparkSession().sql(sql)
        return df
		
def dateFormat(dateStr,oldFormt,newFormt):
    #format=%Y%m%d
    try:
        dateObj=datetime.datetime.strptime(str(dateStr),oldFormt)
    except ValueError:
        try:
             dateObj=datetime.datetime.strptime(str(dateStr),'%d-%b-%y')

        except ValueError:
           try:
                 dateObj=datetime.datetime.strptime(str(dateStr),'%m/%d/%Y')
           except ValueError:
                 dateObj=datetime.datetime.strptime(str(dateStr),'%Y%m%d')

    return dateObj.strftime(newFormt)	

	
def getPrevDate(currentDate,noDays):
       qDate=datetime.datetime.strptime(currentDate,'%Y%m%d') - datetime.timedelta(days=noDays)
       sdate=str(qDate).split(' ')
       sd = sdate[0].split('-')
       sds=sd[0]+sd[1]+sd[2]
       return str(sds)


def getCurrDate():
       currentDate=datetime.datetime.now().strftime("%Y%m%d")
       filesDate=datetime.datetime.strptime(currentDate, '%Y%m%d')-timedelta(days=int(0))
       sdate=str(filesDate).split(' ')
       sd = sdate[0].split('-')
       sds=sd[0]+sd[1]+sd[2]
       return str(sds)

def dataConcat(cola,consym,colb):
    return cola+consym+colb

def randomString(stringLength=2):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

def getSequence():
    seq=random.randint(1,100)
    return str(seq)

def regMemTables():
      print("Registering UDF in memory ")
      getSparkSession().udf.register("getPrevDate", getPrevDate, StringType())
      getSparkSession().udf.register("dateFormat", dateFormat, StringType())
      getSparkSession().udf.register("dataConcat", dataConcat, StringType())
      getSparkSession().udf.register("randomString", randomString, StringType())


def getCreateTempView(dd,tbl_name):
       dd.createOrReplaceTempView(tbl_name)


  
#\"+str(initDate)+"'  \


	   
		
def getQuniqueData(initDate):
        regMemTables()
        print('Query Qnique table')
        qsql="SELECT Distinct CONCAT(signoff_date,'_',date_of_call,'_',lower(call_reference)) as row_key, signoff_date,agent_name, team_name,username,department_name,coach_name,\
           call_type,date_of_call,call_reference,\
           CASE WHEN  (REGEXP_REPLACE(replace(child_quiz_name,' ','_'),'[^a-z0-9A-Z_]','') RLIKE '^[0-9]_') = true THEN \
                lower(SUBSTR(REGEXP_REPLACE(replace(child_quiz_name,' ','_'),'[^a-z0-9A-Z_]',''),\
                     2-length(REGEXP_REPLACE(replace(child_quiz_name,' ','_'),'[^a-z0-9A-Z_]','')))) \
           WHEN (REGEXP_REPLACE(replace(child_quiz_name,' ','_'),'[^a-z0-9A-Z_]','') RLIKE  '^[0-9]') = true THEN \
         	       lower(SUBSTR(REGEXP_REPLACE(replace(child_quiz_name,' ','_'),'[^a-z0-9A-Z_]',''),\
                            1-length(REGEXP_REPLACE(replace(child_quiz_name,' ','_'),'[^a-z0-9A-Z_]',''))))\
           ELSE \
 	        lower(REGEXP_REPLACE(replace(child_quiz_name,' ','_'),'[^a-z0-9A-Z_]',''))\
           END as child_quiz_name,\
           CAST(child_eval_perc as INT) as child_eval_perc,total_score,parent_quiz_name,duration_of_call \
           FROM vw_agent_appraisal \
           WHERE signoff_date='"+str(initDate)+"' and coach_code=fk_user_evaluator_code \
           and parent_quiz_name not in ('NW Retention','TMPESA Measures','Previous First Line Template','WOW Factor - 2017',\
           'First Line Service Request','TPostpaid Measures','NW CQ Template (April 2013)','TPrepaid Measures','Previous Call Quality Template',\
           'xSR - First Line','TInterpersonal Measures','Experience Excellence 2017','NW Directory Template','RT - IVR Repeat Caller Script Adherence',\
          'Dealer Outbound - Interpersonal','Call Quality Template','Interpersonal Skills','NW SMS Template','NW Care Desk Template',\
          'xPrepaid Measures','NW CQ Template','CareDesk Interpersonal Skills','xInterpersonal Skills','xMPESA Measures','Directory',\
          'NW Second Line SR','Pre-paid Measures','Service Request (1st Line) Template','RT - Interpersonal Skills',\
          'Platinum Excellence Template Mar 2018','Business Measures -2017','Interpersonal Measures - 2017','NW eMail Template',\
          'NW Reversal Template (Oct 2016)','Reversals  - Interpersonal Skills (2016)','xSMS Measures','CareDesk zBusiness Measures',\
          'RT - Surveys','MPESA Measures')"
        print(qsql)
        withDF=getReadQniqueCassandra(qsql)
        #df=getCreateTempView(withDF,"tbl_name")	
        #withDF=getSparkSession().sql("SELECT child_quiz_name_org, child_quiz_name  FROM tbl_name where child_quiz_name like '%rapport%'")
        #withDF.show(20,False) 
	#withDF=resultDF.withColumn('child_quiz_name',regexp_replace(col('child_quiz_name')," ","_"))
        pivotedDF=withDF.groupBy(withDF['row_key'],withDF['signoff_date'],withDF['team_name'],withDF['agent_name'],\
		withDF['department_name'],withDF['coach_name'],withDF['call_type'],withDF['username'],withDF['date_of_call'],\
		withDF['duration_of_call'],withDF['call_reference'],withDF['total_score'])\
		.pivot('child_quiz_name').sum('child_eval_perc')
        #pivotedDF.show(50,False)
	return pivotedDF
        #WHERE signoff_date between '20191017' and  '"+str(initDate)+"'  \
         #and substring(date_of_call, 1, 6)=substring("+str(initDate)+",1,6)\

		

       

indate=getPrevDate(str(getCurrDate()),1)
print('Running for date : '+ indate)       
df=getQuniqueData(indate)
getWriteTable(df,'appraisal_score_cards','qnique')
