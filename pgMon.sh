#!/bin/bash

usage()
{
cat << EOF
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

EOF
}

setup() {
cat << EOF
BEGIN;

CREATE TABLE serverlist (
    id serial primary key,
    name text,
    ip text,
    code text,
    created_at timestamp
);

CREATE TABLE dblist (
    id serial primary key,
    serverid integer references serverlist(id),
    name text,
    code text unique,
    created_at timestamp
);

CREATE TABLE tablelist (
    id serial primary key,
    datid integer references dblist(id),
    name text,
    code text unique,
    created_at timestamp  
);

CREATE TABLE constraintlist (
    id serial primary key,
    relid integer references tablelist(id),
    name text,
    code text unique,
    created_at timestamp  
);

CREATE TABLE dumpsizes (
    id serial primary key,
    datid integer references dblist(id),
    dumpname text,
    size bigint,
    created_at timestamp
);

CREATE TABLE dbstats (
    id serial primary key,
    datid integer references dblist(id),
    inserted integer,
    deleted integer,
    updated integer,
    commit integer,
    rollback integer,
    created_at timestamp
);

CREATE TABLE tablestats (
    id serial primary key,
    relid integer references tablelist(id),
    autovacuum bigint,
    last_analyze timestamp,
    last_autovacuum timestamp,
    created_at timestamp
);

CREATE TABLE growth_ratio_db (
    id serial primary key,
    datid integer references dblist(id),
    size bigint,
    created_at timestamp
);

CREATE TABLE growth_ratio_table (
    id serial primary key,
    relid integer references tablelist(id),
    size bigint,
    created_at timestamp
);

CREATE TABLE growth_ratio_index (
    id serial primary key,
    relid integer references constraintlist(id),
    size bigint,
    created_at timestamp
);

CREATE TABLE bgwriterstats (
    id serial primary key,
    serverid integer references serverlist(id),
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time bigint,
    checkpoint_sync_time bigint,
    buffers_checkpoint bigint,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint,
    stats_reset timestamp,
    created_at timestamp
);

CREATE TABLE indexstats (
    id serial primary key,
    relid integer references constraintlist(id),
    size bigint,
    usage bigint,
    ratio numeric(7,2),
    created_at timestamp
);

CREATE TABLE checkpointstats (
    id serial primary key,
    serverid integer references serverlist(id),
    checkpoints_timed bigint,
    checkpoints_req bigint,
    created_at timestamp
);

CREATE TABLE repstats (
    id serial primary key,
    serverid integer references serverlist(id),
    rep_time timestamp,
    rep_delay timestamp,
    created_at timestamp
);

CREATE TABLE query_heap_blks_stats (
    id serial primary key,
    relid integer references tablelist(id),
    hit_pct decimal(7,2),
    heap_blks_hit bigint,
    heap_blks_read bigint,
    created_at timestamp
);

CREATE TABLE query_disc_cache_stats (
    id serial primary key,
    relid integer references tablelist(id),
    heap_from_disc bigint,
    heap_from_cache bigint,
    index_from_disc bigint,
    index_from_cache bigint,
    toast_from_disc bigint,
    toast_from_cache bigint, 
    toast_index_disc bigint,
    toast_index_cache bigint,
    created_at timestamp
);

CREATE TABLE query_bloat_stats (
    id serial primary key,
    relid integer references tablelist(id),
    tbloat decimal(7,1),
    wastedbytes bigint,
    consid integer references constraintlist(id),
    ibloat decimal(7,1),
    wastedibytes bigint,
    created_at timestamp
);

CREATE OR REPLACE FUNCTION insert_data(type text, servercode text, tbl text, cols text, data text) RETURNS VOID AS \$\$
DECLARE
    id integer;
    reltbl text;
    relcol text;
BEGIN
        IF type = 'db' THEN
            reltbl := 'dblist';
            relcol := 'datid';
        ELSIF type = 'table' THEN
            reltbl := 'tablelist';
            relcol := 'relid';
        ELSIF type = 'constraint' THEN
            reltbl := 'constraintlist';
            relcol := 'relid';
        ELSIF type = 'server' THEN
            reltbl := 'serverlist';
            relcol := 'serverid';
        END IF;
        
        EXECUTE 'SELECT id FROM '||reltbl||' WHERE code='''||servercode||'''' INTO id;
        EXECUTE FORMAT('INSERT INTO %s(%s,%s) VALUES(%s,%s)',tbl,relcol,cols,id,data);

END;
\$\$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION insert_data2(type text, tbl text, relcode text, conscode text, relcols text, conscols text, reldata text, consdata text) RETURNS VOID AS \$\$
DECLARE
    relid integer;
    consid integer;
    reltbl text;
    relcol text;
    conscol text;
BEGIN
        relcol := 'relid';
        conscol := 'consid';
        
        IF conscode = '?' THEN
            EXECUTE 'SELECT id FROM tablelist WHERE code='''||relcode||'''' INTO relid;
            EXECUTE FORMAT('INSERT INTO %s(%s,%s,%s) VALUES(%s,%s,%s)',tbl,relcol,relcols,conscols,relid,reldata,consdata);
        ELSE
            EXECUTE 'SELECT id FROM constraintlist WHERE code='''||conscode||'''' INTO consid ;
            EXECUTE 'SELECT id FROM tablelist WHERE code='''||relcode||'''' INTO relid;
            EXECUTE FORMAT('INSERT INTO %s(%s,%s,%s,%s) VALUES(%s,%s,%s,%s)',tbl,relcol,relcols,conscol,conscols,relid,reldata,consid,consdata);
        END IF;
        
END;
\$\$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION add_server(servercode text, hostname text, ip text) RETURNS VOID AS
\$\$
DECLARE
    scode text;
BEGIN
        SELECT code INTO scode FROM serverlist WHERE code=servercode;
        IF scode IS NULL THEN
            --RAISE NOTICE '% ; % ; %',servercode,hostname,ip;
            EXECUTE FORMAT('INSERT INTO serverlist(name,ip,code,created_at) VALUES (''%s'',''%s'',''%s'',now())',hostname,ip,servercode);
        ELSE
            RAISE NOTICE '% is exists',servercode;
        END IF;
END;
\$\$ LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION add_item(type text, servercode text, hostname text, relcode text) RETURNS VOID AS
\$\$
DECLARE
    scode text;
    tbl text;
    reltbl text;
    col text;
    result integer;
    rid int;
BEGIN
        IF type = 'db' THEN
            tbl := 'dblist';
            reltbl :='serverlist';
            col := 'serverid';
        ELSIF type = 'table' THEN
            tbl := 'tablelist';
            reltbl := 'dblist';
            col := 'datid';
        ELSIF type = 'constraint' THEN
            tbl := 'constraintlist';
            reltbl := 'tablelist';
            col := 'relid';
        END IF;
        
        EXECUTE 'SELECT code FROM '||tbl||' WHERE code='''||servercode||'''';
        GET DIAGNOSTICS result = ROW_COUNT;
        IF result = 0 THEN
            EXECUTE 'SELECT id FROM '||reltbl||' WHERE code='''||relcode||'''' INTO rid ;
            IF rid IS NOT NULL THEN
                EXECUTE FORMAT('INSERT INTO %s(%s,name,code,created_at) VALUES (%s,''%s'',''%s'',now())',tbl,col,rid,hostname,servercode);
            END IF;
            --RAISE NOTICE '% ; % ;;; % ; % ',rid,relcode,servercode,hostname;
        ELSE
            RAISE NOTICE '% is exists',servercode;
        END IF;
END;
\$\$ LANGUAGE 'plpgsql';

COMMIT;
EOF
}

created_at=$(date +%F" "%X)
hostname=`hostname`
user="postgres"
pgmonuser="pgmon_user"
pgmondb="pgmon"
pgmonhost="10.22.22.97"
#pgmonhost="10.22.2.112"
db=""
param="$@"
version="9.1.6"
#basedir="/home/travego/pgMon"
basedir=`pwd`
errlog="$basedir/pgmon.err.log"x
log="$basedir/pgmon.log"
backupdir="$basedir/tmp"
db_list="'template0','template1','postgres'"

ErrorCheck()
{
    if [ $? -gt 0 ]
    then
        now=`date "+%H:%M:%S %d-%m-%y"`
        echo "ERROR: occured in \"$1\""
        #echo "ERROR: occured in \"$1\" at $now" >> $errlog
        exit
    fi
}

function getIP()
{
    if [ "$clientIP" = "" ]
    then
        clientIP=`/sbin/ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'`
    fi
    if [ "$clientIP" = "" ]
    then
        echo "The IP address of this machine is not found. Use --clientip parameter to define IP address."
        exit
    fi
}

if [ $# -eq 0 ]
then
    usage
fi


#clientIP="10.22.2.76"
#hostname="travijuu"

while [ $# -gt 0 ]
do
    exp=$(echo "$1" | grep '^-[[:lower:]]')
    if [ "$1" = "$exp" ]
    then
        getIP
        case "$1" in 
        "-h" | "-help")
            usage
            exit
            ;;
        "-dump")
            if ! [ "$db" = "" ]
            then
                db_arr=$(echo $db | tr "," "\n" )
            else
                db_names=`psql -U $user -At -c "SELECT array_to_string(ARRAY(SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1','postgres')),'-');"`
                db_arr=$(echo $db_names | tr "-" "\n" )
            fi
            for name in ${db_arr[*]}
            do
                dumpname=$(date +%a)-$name.dump
                if [ -e "$backupdir/$dumpname" ]
                then
                    size=`ls -l $backupdir/$dumpname | awk '{print $5}'`
                    echo "Database: $name | Size: $size kb"
                    psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('db','$clientIP-$hostname-$name','dumpsizes','dumpname, size, created_at','''$dumpname'',$size,''now()''')" > /dev/null #2>> $errlog
                else
                    echo "$backupdir/$dumpname: File Not Found"
                fi
            done
            echo "--> Dump is OK."
            ;;
        "-growth-ratio-db")
            if [ "$db" = "" ]
            then
                result=`psql -U $user $i -At -R ";" -c "SELECT datname, pg_database_size(oid) || ',''''' || now() || '''''' FROM pg_database WHERE datname NOT IN ('template0','template1','postgres')"`
            else
                db=`echo $db | sed "s/,/\',\'/g" | sed "s/^/\'/" | sed "s/$/\'/" `
                result=`psql -U $user $i -At -R ";" -c "SELECT datname, pg_database_size(oid) || ',''''' || now() || '''''' FROM pg_database WHERE datname IN ( $db )"`
            fi

            ErrorCheck $1
            export IFS=";"
            for row in $result
            do
                echo $row
                dbname=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                row=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('db','$clientIP-$hostname-$dbname','growth_ratio_db','size,created_at','$row')" > /dev/null
                ErrorCheck $1
            done
            echo "--> Database growth ratio is OK."
            ;;
        "-growth-ratio-table")
    
            if ! [ "$db" = "" ]
            then
                db_arr=$(echo $db | tr "," "\n" )
            else
                db_names=`psql -U $user -At -c "SELECT array_to_string(ARRAY(SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1','postgres')),'-');"`
                db_arr=$(echo $db_names | tr "-" "\n" )
            fi
            for dbname in ${db_arr[*]}
            do
                echo "Database: $dbname" 
                echo "---------"
                result=`psql -U $user $dbname -At -R ";" -c "select t2.relname,pg_relation_size(t2.oid) || ',''''' || now() || '''''' from pg_stat_user_tables t1, pg_class t2 where t1.relname=t2.relname"` > /dev/null
                ErrorCheck $1
                export IFS=";"
                for row in $result
                do
                    echo $row
                    tablename=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                    row=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                    psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('table','$clientIP-$hostname-$dbname-$tablename','growth_ratio_table','size,created_at','$row') " > /dev/null
                    ErrorCheck $1
                done
                echo ""
            done
            echo "--> Table growth ratio is OK."
            ;;
        "-growth-ratio-index")
             
            if ! [ "$db" = "" ]
            then
                db_arr=$(echo $db | tr "," "\n" )
            else
                db_names=`psql -U $user -At -c "SELECT array_to_string(ARRAY(SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1','postgres')),'-');"`
                db_arr=$(echo $db_names | tr "-" "\n" )
            fi
            for dbname in ${db_arr[*]}
            do
                result=`psql -U $user $dbname -At -R ";" -c "SELECT t1.indexrelname,pg_relation_size(t2.oid) || ',''''' || now() || '''''' from pg_stat_user_indexes t1, pg_class t2 where  t1.relname=t2.relname ;"`
                ErrorCheck $1
                export IFS=";"
                for row in $result
                do
                    echo $row
                    indexname=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                    row=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                    psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('constraint','$clientIP-$hostname-$dbname-$indexname','growth_ratio_index','size,created_at','$row') " > /dev/null
                 
                    ErrorCheck $1
                done
            done
            echo "--> Index growth ratio is OK."
            ;;
        "-tablestats")

            if ! [ "$db" = "" ]
            then
                db_arr=$(echo $db | tr "," "\n" )
            else
                db_names=`psql -U $user -At -c "SELECT array_to_string(ARRAY(SELECT datname FROM pg_database WHERE datname NOT IN ( $db_list )),'-');"`
                db_arr=$(echo $db_names | tr "-" "\n" )
            fi
            for dbname in ${db_arr[*]}
            do 
                result=`psql -U $user $dbname -At -R ";" -c "SELECT relname, autovacuum_count || ',''''' || coalesce(last_analyze,'1970-01-01 00:00:00+02') || ''''',''''' || coalesce(last_autovacuum,'1970-01-01 00:00:00+02') || ''''',''''' || now() || '''''' FROM pg_stat_user_tables;"`
                ErrorCheck $1
                export IFS=";"
                for row in $result
                do
                    row=`echo $row | sed "s/''1970-01-01 00:00:00+02''/NULL/g"`
                    echo $row
                    tablename=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                    row=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                    psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('table','$clientIP-$hostname-$dbname-$tablename','tablestats','autovacuum,last_analyze,last_autovacuum,created_at','$row')" > /dev/null
                    ErrorCheck $1
                done
               
            done
            echo "--> Table statistics is OK."
            ;;
        "-dbstats")

            if [ "$db" = "" ]
            then
                result=`psql -U $user -At -R ";"  -c "SELECT datname, tup_inserted || ',' || tup_deleted || ',' || tup_updated || ',' || xact_commit || ',' || xact_rollback || ',''''now()''''' FROM pg_stat_database WHERE datname NOT IN ( $db_list ) ;"`
            else
                db_list=`echo $db | sed "s/,/\',\'/g" | sed "s/^/\'/" | sed "s/$/\'/" `
                result=`psql -U $user -At -R ";"  -c "SELECT datname, tup_inserted || ',' || tup_deleted || ',' || tup_updated || ',' || xact_commit || ',' || xact_rollback || ',''''now()''''' FROM pg_stat_database WHERE datname IN ( $db_list ) ;"`
            fi

            export IFS=";"
            for row in $result
            do
                echo $row
                dbname=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                row=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                psql -U $pgmonuser -h $pgmonhost $pgmondb -At -c "SELECT insert_data('db','$clientIP-$hostname-$dbname','dbstats','inserted, deleted, updated, commit, rollback,created_at','$row');" > /dev/null
                ErrorCheck $1
            done

            echo "--> DB statistics is OK."
            ;;
        "-indexstats")
            
            if ! [ "$db" = "" ]
            then
                db_arr=$(echo $db | tr "," "\n" )
            else
                db_names=`psql -U $user -At -c "SELECT array_to_string(ARRAY(SELECT datname FROM pg_database WHERE datname NOT IN ( $db_list )),'-');"`
                db_arr=$(echo $db_names | tr "-" "\n" )
            fi
            
            for dbname in ${db_arr[*]}
            do
                result=`psql -U $user -At -R ";" $dbname  -c "SELECT t2.indexrelname, pg_relation_size(t3.oid) || ',' || case when pg_relation_size(t4.oid) != 0  then (pg_relation_size(t3.oid)*1.0/pg_relation_size(t4.oid)*100)::numeric(7,2) else 0 end || ',' || t2.idx_scan || ',''''' || now() || '''''' from pg_stat_user_tables t1 join pg_stat_user_indexes t2 on t1.relname=t2.relname join pg_class t3 on t2.indexrelname=t3.relname join pg_class t4 on t1.relname=t4.relname"`
                export IFS=";"
                for row in $result
                do
                    echo $row
                    indexname=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                    row=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                    echo "$clientIP-$hostname-$dbname-$indexname"
                    psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('constraint','$clientIP-$hostname-$dbname-$indexname','indexstats','size,ratio,usage,created_at','$row')"  > /dev/null
                    ErrorCheck $1
                done
            done
            ;;
        "-repstats")
            check=`psql -U $user -At -R ";" -c "SELECT 1 from pg_stat_replication having count(*) > 0"`
            if [ "$check" = "1" ]
            then
                result=`psql -U $user -At -R ";" -c "SELECT ''''' || NOW()-backend_start || ''''',''''' ||  NOW() - coalesce(pg_last_xact_replay_timestamp(),0) || ''''',''''' || NOW() || ''''' from pg_stat_replication;"`
                psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('server','$clientIP-$hostname','repstats','rep_time,rep_delay,created_at','$result');"
                echo "--> Replication is OK."
            else
                echo "Warning: Replication record is not found."
                exit
            fi
            ;;
        "-query")
            sqlArr=( "query_heap_blks_stats|SELECT relname,cast(heap_blks_hit as numeric) / (heap_blks_hit + heap_blks_read) AS hit_pct,heap_blks_hit,heap_blks_read,now() FROM pg_statio_user_tables WHERE (heap_blks_hit + heap_blks_read)>0 ORDER BY hit_pct;|server,datname,relname,hit_pct,heap_blks_hit,heap_blks_read,created_at"
                     "query_disc_cache_stats|SELECT relname, coalesce( heap_blks_read,0) || '','' || heap_blks_hit || ',' || coalesce(idx_blks_read,0) || ',' || coalesce(idx_blks_hit,0) || ',' || coalesce(toast_blks_read,0) || ',' || coalesce(toast_blks_hit,0) || ',' || coalesce(tidx_blks_read,0) || ',' || coalesce(tidx_blks_hit,0) || ',''''' || now() || '''''' from pg_statio_user_tables where relname = '$table'; | server,datname,relname,\"heap from disc\",\"heap from cache\",\"index from disc\",\"index from cache\",\"toast from disc\",\"toast from cache\",\"toast index disc\",\"toast index cache\",created_at|"
                     "query_bloat_stats|SELECT current_database() as datname, schemaname, tablename, ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat, CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END AS wastedbytes, iname, ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat, CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes, now() as created_at FROM ( SELECT schemaname, tablename, cc.reltuples, cc.relpages, bs, CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta, COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta FROM ( SELECT ma,bs,schemaname,tablename, (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr, (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum, hdr+( SELECT 1+count(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::numeric) AS bs, CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants GROUP BY 1,2,3,4,5 ) AS foo ) AS rs JOIN pg_class cc ON cc.relname = rs.tablename JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid ) AS sml WHERE schemaname !='pg_catalog' ORDER BY wastedbytes DESC;|server,datname,schema,tablename,tbloat,wastedbytes,iname,ibloat,wastedibytes,created_at"
                   )
            for sql in "${sqlArr[@]}"
            do
                qname=$(echo $sql | awk 'BEGIN{FS="|"}{print $1}')
                if [ "$name" = "$qname" ]
                then
                    query=$(echo $sql | awk 'BEGIN{FS="|"}{print $2}')
                    columns=$(echo $sql | awk 'BEGIN{FS="|"}{print $3}')
                    replace=$(echo $sql | awk 'BEGIN{FS="|"}{print $4}')
                    echo $query
                    tablename=`echo $query | awk 'BEGIN{FS="|"}{print $1}'`
                    query=`echo $query | awk 'BEGIN{FS="|"}{print $2}'`
                    echo "ok"
                    result=`psql -U $user -R ";" $db -At -c "$query"`

                    ErrorCheck $1
                    export IFS=";"
                    for row in $result
                    do
                        echo $row
                        psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('table','$clientIP-$hostname-$dbname-$tablename','$qname','$columns','$row')"
                        ErrorCheck $1
                    done
                fi
            done
            echo "--> Query is OK."
            ;;
        "-heap_blks_stats")
            
            if ! [ "$db" = "" ]
            then
                db_arr=$(echo $db | tr "," "\n" )
            else
                db_names=`psql -U $user -At -c "SELECT array_to_string(ARRAY(SELECT datname FROM pg_database WHERE datname NOT IN ( $db_list )),'-');"`
                db_arr=$(echo $db_names | tr "-" "\n" )
            fi
            for dbname in ${db_arr[*]}
            do 
                result=`psql -U $user -R ";" $dbname -At -c "SELECT relname, (cast(heap_blks_hit as numeric)  / (heap_blks_hit + heap_blks_read) )::numeric(7,3)   || ',' || heap_blks_hit || ',' || heap_blks_read || ',''''' || now() || '''''' FROM pg_statio_user_tables WHERE (heap_blks_hit + heap_blks_read)>0;"`
                export IFS=";"
                for row in $result
                do
                    tablename=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                    echo $row
                    row=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                    psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('table','$clientIP-$hostname-$dbname-$tablename','query_heap_blks_stats','hit_pct,heap_blks_hit,heap_blks_read,created_at','$row')" > /dev/null
                    ErrorCheck $1
                done
            done
            echo "--> heap blks stats is OK."
            ;;
        "-disc_cache_stats")
            if ! [ "$table" = "" ]
            then
                row=`psql -U $user -R ";" $db -At -c "SELECT relname, coalesce( heap_blks_read,0) || ',' || heap_blks_hit || ',' || coalesce(idx_blks_read,0) || ',' || coalesce(idx_blks_hit,0) || ',' || coalesce(toast_blks_read,0) || ',' || coalesce(toast_blks_hit,0) || ',' || coalesce(tidx_blks_read,0) || ',' || coalesce(tidx_blks_hit,0) || ',''''' || now() || '''''' from pg_statio_user_tables where relname = '$table';"`
                tablename=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                echo $row
                row=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('table','$clientIP-$hostname-$db-$tablename','query_disc_cache_stats','heap_from_disc,heap_from_cache,index_from_disc,index_from_cache,toast_from_disc,toast_from_cache,toast_index_disc,toast_index_cache,created_at','$row')" > /dev/null
            else
                if ! [ "$db" = "" ]
                then
                    db_arr=$(echo $db | tr "," "\n" )
                else
                    db_names=`psql -U $user -At -c "SELECT array_to_string(ARRAY(SELECT datname FROM pg_database WHERE datname NOT IN ( $db_list )),'-');"`
                    db_arr=$(echo $db_names | tr "-" "\n" )
                fi
                for dbname in ${db_arr[*]}
                do
                                                    
                    result=`psql -U $user -R ";" $dbname -At -c "SELECT relname, coalesce( heap_blks_read,0) || ',' || heap_blks_hit || ',' || coalesce(idx_blks_read,0) || ',' || coalesce(idx_blks_hit,0) || ',' || coalesce(toast_blks_read,0) || ',' || coalesce(toast_blks_hit,0) || ',' || coalesce(tidx_blks_read,0) || ',' || coalesce(tidx_blks_hit,0) || ',''''' || now() || '''''' from pg_statio_user_tables;"`
                    export IFS=";"
                    for row in $result
                    do
                        tablename=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                        echo $row
                        row=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                        psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('table','$clientIP-$hostname-$dbname-$tablename','query_disc_cache_stats','heap_from_disc,heap_from_cache,index_from_disc,index_from_cache,toast_from_disc,toast_from_cache,toast_index_disc,toast_index_cache,created_at','$row')" > /dev/null
                        ErrorCheck $1
                    done
                done
            fi
            ;;
        "-bloat_stats")
            
            if ! [ "$db" = "" ]
            then
                db_arr=$(echo $db | tr "," "\n" )
            else
                db_names=`psql -U $user -At -c "SELECT array_to_string(ARRAY(SELECT datname FROM pg_database WHERE datname NOT IN ( $db_list )),'-');"`
                db_arr=$(echo $db_names | tr "-" "\n" )
            fi
            for dbname in ${db_arr[*]}
            do 
                result=`psql -U $user -R ";" $dbname -At -c "SELECT tablename, ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) || ',' || CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END, iname, ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) || ',' || CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END  || ',''''' || now() || ''''''  FROM ( SELECT schemaname, tablename, cc.reltuples, cc.relpages, bs, CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta, COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta FROM ( SELECT ma,bs,schemaname,tablename, (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr, (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum, hdr+( SELECT 1+count(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::numeric) AS bs, CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants GROUP BY 1,2,3,4,5 ) AS foo ) AS rs JOIN pg_class cc ON cc.relname = rs.tablename JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid ) AS sml WHERE schemaname !='pg_catalog';"`
                export IFS=";"
                for row in $result
                do
                    echo $row
                    tablename=`echo $row | awk 'BEGIN{FS="|"}{print $1}'`
                    row1=`echo $row | awk 'BEGIN{FS="|"}{print $2}'`
                    indexname=`echo $row | awk 'BEGIN{FS="|"}{print $3}'`
                    row2=`echo $row | awk 'BEGIN{FS="|"}{print $4}'`
                    if [ "$indexname" = "?" ]
                    then
                        indexcode="?"
                    else
                        indexcode="$clientIP-$hostname-$dbname-$indexname"
                    fi
                    psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data2('table','query_bloat_stats','$clientIP-$hostname-$dbname-$tablename','$indexcode','tbloat,wastedbytes','ibloat,wastedibytes,created_at','$row1','$row2')" > /dev/null
                    ErrorCheck $1
                done
            done
            ;;
        "-bgwriter")
            db_ver=`psql -U $user -At -c "SELECT version()" | awk '{print $2}'`

            if [[ "$db_ver" < "9.1" ]]
            then
                echo "ERROR: $db_ver is not a supported version."
                exit;
            elif [[ "$db_ver" < "9.2" ]]
            then
                result=`psql -U $user -At -c "SELECT checkpoints_timed || ',' || checkpoints_req || ',' || -1 || ',' || -1 || ',' || buffers_checkpoint || ',' || buffers_clean || ',' || maxwritten_clean || ',' || buffers_backend || ',' || buffers_backend_fsync || ',' || buffers_alloc || ',''''' || stats_reset || ''''',''''' || now() || '''''' FROM pg_stat_bgwriter;"`    
            else
                result=`psql -U $user -At -c "SELECT checkpoints_timed || ',' || checkpoints_req || ',' || checkpoint_write_time || ',' || checkpoint_sync_time || ',' || buffers_checkpoint || ',' || buffers_clean || ',' || maxwritten_clean || ',' || buffers_backend || ',' || buffers_backend_fsync || ',' || buffers_alloc || ',''''' || stats_reset || ''''',''''' || now() || '''''' FROM pg_stat_bgwriter;"`
            fi

            psql -U $pgmonuser -h $pgmonhost $pgmondb -At -c "SELECT insert_data('server','$clientIP-$hostname','bgwriterstats','checkpoints_timed,checkpoints_req,checkpoint_write_time,checkpoint_sync_time,buffers_checkpoint,buffers_clean,maxwritten_clean,buffers_backend,buffers_backend_fsync,buffers_alloc,stats_reset,created_at','$result');" > /dev/null
            echo "--> bgwriter is OK."
            ;;
        "-checkpointstats")
            result=`psql -U $user -At -R ";" -c "SELECT checkpoints_timed || ',' || checkpoints_req || ',''''' || now() || '''''' FROM pg_stat_bgwriter;"`
            export IFS=";"
            for row in $result
            do
                echo $row
                psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT insert_data('server','$clientIP-$hostname','checkpointstats','checkpoints_timed,checkpoints_req,created_at','$result'); " > /dev/null
                ErrorCheck $1
            done
            echo "--> Checkpoint is OK."
            ;;
        "-version")
            db_ver=`psql -U $user -At -c "SELECT version()" | awk '{print $2}'`
            if [[ "$db_ver" < "$version" ]]
                then
                echo "$db_ver is not supported.."
                exit
            fi
            echo "PostgreSQL version: $db_ver - supported version"
            ;;
        "-setup")
            $0 -version
    
            curr_db=`psql -U $user -At -c "SELECT datname FROM pg_database WHERE datname='$pgmondb'"`
            if [ "$curr_db" = "$pgmondb" ]
            then
                echo "> ERROR: The database \"$pgmondb\" is exist."
                echo -n "Would you like to drop $pgmondb?[y/n]: "
                read answer
                if [ "$answer" = "y" ]
                then
                    psql -U $user -c "DROP DATABASE $pgmondb";
                    echo "$pgmondb is dropped."
                    psql -U $user -c "CREATE ROLE $pgmonuser LOGIN;"
                    psql -U $user -c "CREATE DATABASE $pgmondb OWNER $pgmonuser;"
                    setup | psql -U $pgmonuser $pgmondb  
                    echo "--> pgMon setup is completed."
                else
                    echo "$pgmondb is not dropped."
                    exit
                fi
            fi          
            ;;
        "-init")
            echo "--> Database initializion is started"
            psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT add_server('$clientIP-$hostname','$hostname','$clientIP')" > /dev/null

            if ! [ "$db" = "" ]
            then
                db_arr=$(echo $db | tr "," "\n" )
            else
                db_names=`psql -U $user -At -c "SELECT array_to_string(ARRAY(SELECT datname FROM pg_database WHERE datname NOT IN ( $db_list )),'-');"`
                db_arr=$(echo $db_names | tr "-" "\n" )
                
            fi
          
            echo $db_arr
            for dbname in ${db_arr[*]}
            do
                echo "Database: $dbname"
                psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT add_item('db','$clientIP-$hostname-$dbname','$dbname','$clientIP-$hostname')" > /dev/null
                table_names=`psql -U $user -At $dbname -c "SELECT array_to_string(ARRAY(SELECT relname FROM pg_stat_user_tables),'-');"`
                table_arr=$(echo $table_names | tr "-" "\n" )
                for tablename in ${table_arr[*]}
                do
                    echo "++ Table: $tablename"
                    psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT add_item('table','$clientIP-$hostname-$dbname-$tablename','$tablename','$clientIP-$hostname-$dbname')" > /dev/null
                    index_names=`psql -U $user -At $dbname -c "SELECT array_to_string(ARRAY(SELECT indexrelname FROM pg_stat_user_indexes WHERE relname='$tablename'),'-');"`
                    index_arr=$(echo $index_names | tr "-" "\n" )
                    for indexname in ${index_arr[*]}
                    do
                        echo "++++ Constraint: $indexname"
                        psql -U $pgmonuser -h $pgmonhost $pgmondb -c "SELECT add_item('constraint','$clientIP-$hostname-$dbname-$indexname','$indexname','$clientIP-$hostname-$dbname-$tablename')" > /dev/null
                    done
                done
            done
            echo "--> pgMon is initialized."
            ;;
        "-checkconnection")
            psql -U $pgmonuser -At -h $pgmonhost $pgmondb -c "SELECT version();"
            ;;
        *)
            echo "Unknown option: $1"
            exit
            ;;
        esac
        shift
    elif [ "$1" = $(echo "$1" | grep '^--[[:lower:]]') ]
    then
        case "$1" in
        "--db")
            db=$2
            ;;
        "--table")
            table=$2
            ;;
        "--host")
            pgmonhost=$2
            ;;
        "--name")
            name=$2
            ;;
        "--backupdir")
            backupdir=$2
            ;;
        "--clientip")
            clientip=$2
            ;;
        "--hostip")
            pgmonhost=$2
            ;;
        *)
            echo "Unknown option: $1"
            exit
            ;;
        esac
        echo "  $1 is setted to $2"
        shift 2
    else
        echo "Error $1: wrong parameter"    
        exit
    fi
done