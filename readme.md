deneme

Name:     pgMon
Description:
          pgMon is a monitor tool for PostgreSQL.
Usage:
          ./pgMon.sh [ --db db1,db2,.. ] [ --backupdir dir ] -dump
          ./pgMon.sh [ --db db1,db2,.. ] -growth-ratio-db
          ./pgMon.sh [ --db db1,db2,.. ] -growth-ratio-table
          ./pgMon.sh [ --db db1,db2,.. ] -growth-ratio-index
          ./pgMon.sh [ --db db1,db2,.. ] -tablestats
          ./pgMon.sh [ --db db1,db2,.. ] -dbstats
          ./pgMon.sh [ --db db1,db2.. ] -heap_blks_stats
          ./pgMon.sh [ --db db1 --table tbl ] -disc_cache_stats
          ./pgMon.sh [ --db db1,db2.. ] -bloat_stats
          ./pgMon.sh -bgwriter
          ./pgMon.sh -checkpointstats
          ./pgMon.sh -version
          ./pgMon.sh -setup
          ./pgMon.sh [ --db db1,db2,.. ] -init
          ./pgMon.sh -checkconnection

     Notes:  *** Setter parameters should be defined before the action parameters   ***
             *** [ .. ] means that these parameters are optional                    ***
OPTIONS:
  ACTION OPTIONS
   -dump                  sends dump files sizes to pgMon database
   -growth-ratio-db       sends sizes of all OR defined databases to pgMon database for growth ratio
   -growth-ratio-table    sends sizes of all tables in all OR defined databases to pgMon database for growth ratio
   -growth-ratio-index    sends sizes of all indexes in all OR defined databases to pgMon database for growth ratio
   -tablestats            sends stats of all tables in all OR defined databases to pgMon database
   -dbstats               sends stats of all or defined databases to pgMon database
   -query                 sends the result of given query in defined database to pgMon database
   -heap_blks_stats
   -disc_cache_stats
   -bloat_stats
   -bgwriter              sends bgwriter stats in this database cluster to pgMon database
   -checkpointstats       
   -version               shows whether the installed PostgreSQL version is supported by pgMon or not 
   -setup                 create pgMon schema in this database machine
                          !!IMPORTANT!! If this machine will be used for gathering information from other database machines, run -setup
   -init                  Sends the structure of all OR defined databases to pgMon database 

  SETTER OPTIONS
   --db          sets database names | can be more than one separated by comma (,)  
   --host        sets pgMon host server
   --table       sets the table name | used with -query option in some queries
   --name        sets the name of the query | used with -query option to define which query will be executed
   --backupdir   sets the backup director path | used with -dump option to define backups directory
   --clientip    sets
   --hostip      sets
