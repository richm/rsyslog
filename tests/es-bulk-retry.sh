#!/bin/bash
# This file is part of the rsyslog project, released under ASL 2.0
export ES_PORT=19200
. $srcdir/diag.sh download-elasticsearch
. $srcdir/diag.sh stop-elasticsearch
. $srcdir/diag.sh prepare-elasticsearch
# change settings to cause bulk rejection errors
cat >> $dep_work_dir/es/config/elasticsearch.yml <<EOF
thread_pool.bulk.queue_size: 1
thread_pool.bulk.size: 1
EOF
. $srcdir/diag.sh start-elasticsearch

. $srcdir/diag.sh init
. $srcdir/diag.sh generate-conf
. $srcdir/diag.sh add-conf '
module(load="../plugins/impstats/.libs/impstats" interval="1"
	   log.file="es-stats.log" log.syslog="off" format="cee")

set $.msgnum = field($msg, 58, 2);
set $.testval = cnum($.msgnum % 2);
if $.testval == 0 then {
	# these should be successful
	set $!msgnum = $.msgnum;
} else {
	# these should cause "hard" errors
	set $!msgnum = "x" & $.msgnum;
}

template(name="tpl" type="string"
    string="{\"msgnum\":\"%$!msgnum%\",\"es_msg_id\":\"%$!es_msg_id%\"}")

module(load="../plugins/omelasticsearch/.libs/omelasticsearch")

template(name="id-template" type="string" string="%$!es_msg_id%")

if strlen($!es_msg_id) == 0 then {
	# NOTE: depends on rsyslog being compiled with --enable-uuid
	set $!es_msg_id = $uuid;
}

ruleset(name="error_es") {
	action(type="omfile" template="RSYSLOG_DebugFormat" file="es-bulk-errors.log")
}

ruleset(name="try_es") {
	if strlen($.omes!status) > 0 then {
		# retry case
		if ($.omes!status == 200) or ($.omes!status == 201) or (($.omes!status == 409) and ($.omes!writeoperation == "create")) then {
			stop # successful
		}
		if ($.omes!writeoperation == "unknown") or (strlen($.omes!error!type) == 0) or (strlen($.omes!error!reason) == 0) then {
			call error_es
			stop
		}
		if ($.omes!status == 400) or ($.omes!status < 200) then {
			call error_es
			stop
		}
		# else fall through to retry operation
	}
	action(type="omelasticsearch"
	       server="127.0.0.1"
	       serverport="'${ES_PORT:-19200}'"
	       template="tpl"
	       writeoperation="create"
	       bulkid="id-template"
	       dynbulkid="on"
	       bulkmode="on"
	       retryfailures="on"
	       retryruleset="try_es"
	       searchType="test-type"
	       searchIndex="rsyslog_testbench")
}

if $msg contains "msgnum:" then {
	call try_es
}

action(type="omfile" file="rsyslog.out.log")
'
rm -f es-bulk-errors.log es-stats.log

curl -s -XPUT localhost:${ES_PORT:-19200}/rsyslog_testbench/ -d '{
  "mappings": {
    "test-type": {
      "properties": {
        "msgnum": {
          "type": "integer"
        }
      }
    }
  }
}
' | python -mjson.tool
#export RSYSLOG_DEBUG="debug nostdout noprintmutexaction"
#export RSYSLOG_DEBUGLOG="debug.log"
if [ "x${USE_VALGRIND:-false}" == "xtrue" ] ; then
	. $srcdir/diag.sh startup-vg
else
	. $srcdir/diag.sh startup
fi
if [ -n "${USE_GDB:-}" ] ; then
	echo attach gdb here
	sleep 54321 || :
fi
numrecords=100
success=50
badarg=50
. $srcdir/diag.sh injectmsg 0 $numrecords
. $srcdir/diag.sh shutdown-when-empty
if [ "x${USE_VALGRIND:-false}" == "xtrue" ] ; then
	. $srcdir/diag.sh wait-shutdown-vg
	. $srcdir/diag.sh check-exit-vg
else
	. $srcdir/diag.sh wait-shutdown
fi
. $srcdir/diag.sh es-getdata $numrecords $ES_PORT
rc=$?

. $srcdir/diag.sh stop-elasticsearch
. $srcdir/diag.sh cleanup-elasticsearch

cat work | \
	python -c '
import sys,json
records = int(sys.argv[1])
expectedrecs = {}
rc = 0
for ii in xrange(0, records*2, 2):
	ss = "{:08}".format(ii)
	expectedrecs[ss] = ss
for item in json.load(sys.stdin)["hits"]["hits"]:
	msgnum = item["_source"]["msgnum"]
	if msgnum in expectedrecs:
		del expectedrecs[msgnum]
	else:
		print "Error: found unexpected msgnum {} in record".format(msgnum)
		rc = 1
for item in expectedrecs:
	print "Error: msgnum {} was missing in Elasticsearch".format(item)
	rc = 1
sys.exit(rc)
' $success || { rc=$?; echo error: did not find expected records in Elasticsearch; }

cat es-stats.log | \
	python -c '
import sys,json
success = int(sys.argv[1])
badarg = int(sys.argv[2])
lasthsh = {}
for line in sys.stdin:
	jstart = line.find("{")
	if jstart >= 0:
		hsh = json.loads(line[jstart:])
		if hsh["name"] == "omelasticsearch":
			lasthsh = hsh
actualsuccess = lasthsh["response.success"]
actualbadarg = lasthsh["response.badargument"]
actualrej = lasthsh["response.bulkrejection"]
actualsubmitted = lasthsh["submitted"]
assert(actualsuccess == success)
assert(actualbadarg == badarg)
assert(actualrej > 0)
assert(actualsuccess + actualbadarg + actualrej == actualsubmitted)
' $success $badarg || { rc=$?; echo error: expected responses not found in stats; }

found=0
for ii in $(seq --format="x%08.f" 1 2 $(expr 2 \* $badarg)) ; do
	if grep -q '^[$][!]:{.*"msgnum": "'$ii'"' es-bulk-errors.log ; then
		found=$( expr $found + 1 )
	else
		echo error: missing message $ii
		rc=1
	fi
done
if [ $found -ne $badarg ] ; then
	echo error: found only $found of $badarg messages in es-bulk-errors.log
	rc=1
fi
if grep -q '^[$][.]:{.*"omes": {' es-bulk-errors.log ; then
	:
else
	echo error: es response info not found in bulk error file
	rc=1
fi
if grep -q '^[$][.]:{.*"status": 400' es-bulk-errors.log ; then
	:
else
	echo error: status 400 not found in bulk error file
	rc=1
fi

if [ $rc -eq 0 ] ; then
	echo tests completed successfully
else
	cat rsyslog.out.log
	. $srcdir/diag.sh error-exit 1 stacktrace
fi

. $srcdir/diag.sh exit
