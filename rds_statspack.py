################## Script to generate spreport for RDS database                   #####################
################## Ora connection details in ora_connection.ini                   ##################### 
################## RDS generates spreport in format <db>_spreport_<id1>_<id2>.lst ##################### 
################## Console log in log.txt
##################  ora_connection.ini contains CONN_STRING="user/pwd@<rds-endpoint>/Dbname"  #########

import oracledb  # pip install oracledb
class Connection(oracledb.Connection):   #DerivedClass(Baseclass)
    dbname=""  
    def __init__(self,file):
        connect_string = self._load_ora_env("ora_connection.ini") # Load oracle connection credentials
        self.file=file                           # filehandler for console log.txt
        try:
            super(Connection, self).__init__(connect_string)
            self._log("connected to db")
        except oracledb.Error as e:
            error_obj,=e.args 
            self._log("Cannot connect to db !\n"+str(error_obj))
            print("Cannot connect to db !")
        
    def _load_ora_env(self,oraenv_file):
        env=open(oraenv_file)
        for line in env: 
          ky=line.split('=',1)[0].strip()
          conn_string=line.split('=',1)[1].strip()
        return  conn_string.replace('"','') #''.join(connect_string)

    def _log(self,message):   
            print(message,file=self.file)   #  Write the console output in log.txt

    def execute(self,sql,parameters):
        with self.cursor() as cur:
         try:
            if parameters is None:
             self._log(sql)
             print("==========List of Snapshots==========")
             for row in cur.execute(sql):   # Query 1
                 print(f"DB:",row[0],",SnapId:", row[1],",Snap_time:",row[2])
                 self._log("DB:"+row[0]+",SnapId:"+ str(row[1])+",Snap_time:"+str(row[2]))
                 Connection.dbname=row[0]
            elif len(parameters)==2:
               spfile=Connection.dbname+'_spreport_'+parameters[0]+'_'+parameters[1]+'.lst'   # Statspack report
               self._log("\nGenerate Statspack for tracefile:"+spfile+ " with snapshot id's: "+parameters[0]+','+parameters[1]+"\n")
               cur.execute(sql,[parameters[0],parameters[1]])  # Block 1  rds_run_spreport and refresh_tracefile_listing
               self._log(sql)  # logs Block 1 
               
               for row in  cur.execute("""select filename from rdsadmin.tracefile_listing where filename like :name """,name=spfile):   # Query 2
                 sql=cur.statement
                 self._log(sql)   # Logs Query 2
               
               cur.execute("""begin rdsadmin.manage_tracefiles.set_tracefile_table_location(:name);end;""",name=spfile)  #Block 2
               sql=cur.statement
               self._log(sql)  # Logs block2
               
               escapes = ''.join([chr(i) for i in range(1, 32)])
               translator = str.maketrans('', '', escapes)
               cur.execute("""SELECT * FROM tracefile_table""")   #Query 3 generates spfile report and writes to spfile
               sql=cur.statement
               self._log(sql)   #Logs Block 3 
               
               with open(spfile,"w",newline='\n') as f:       # Writes query3 result to spfile statspack report
                 print("Generating statspack report -",spfile)
                 for row in cur:
                    if row[0]==None:
                      print('',file=f)
                    else:  
                      line = str(row[0]).translate(translator) # trim escape characters from report
                      print(line,file=f)
         except oracledb.Error as e:
             error_obj,=e.args 
             self._log(error_obj)
             print("Exiting -->",error_obj)
    
file=open ('log.txt',"w")
connect=Connection(file)
connect.execute(''' 
select d.name db,s.SNAP_ID, s.SNAP_TIME FROM STATS$SNAPSHOT s ,v$database d where s.dbid=d.dbid ORDER BY 2 desc fetch  first 20 rows only
''',None)                 #Query 1 with parameters None
begin_snap=input ("\nEnter begin snapshot:")
end_snap=input("Enter end snapshot  :")
connect.execute("""begin rdsadmin.rds_run_spreport(:id1,:id2); rdsadmin.manage_tracefiles.refresh_tracefile_listing; end;""",[begin_snap,end_snap]) # Block 1 with len(parameters)=2
file.close()