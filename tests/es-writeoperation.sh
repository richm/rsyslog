#!/bin/bash
# This file is part of the rsyslog project, released under ASL 2.0
. $srcdir/diag.sh download-elasticsearch
. $srcdir/diag.sh stop-elasticsearch
. $srcdir/diag.sh prepare-elasticsearch
. $srcdir/diag.sh start-elasticsearch

#  Starting actual testbench
. $srcdir/diag.sh init
. $srcdir/diag.sh generate-conf
. $srcdir/diag.sh add-conf '
template(name="tpl" type="string"
	 string="{\"msgnum\":\"%msg:F,58:2%\"}")

module(load="../plugins/omelasticsearch/.libs/omelasticsearch")

if $msg contains "msgnum:" then
	action(type="omelasticsearch"
	       server="127.0.0.1"
	       serverport="19200"
	       template="tpl"
	       writeoperation="create"
	       searchIndex="rsyslog_testbench")

action(type="omfile" file="rsyslog.out.log")
'

# . $srcdir/diag.sh es-init
. $srcdir/diag.sh startup
. $srcdir/diag.sh injectmsg  0 1
. $srcdir/diag.sh shutdown-when-empty
. $srcdir/diag.sh wait-shutdown
if grep -q "omelasticsearch: writeoperation '1' requires bulkid" rsyslog.out.log ; then
	echo found correct error message
else
	echo Error: did not complain about incorrect writeoperation
	cat rsyslog.out.log
	. $srcdir/diag.sh error-exit 1
fi

. $srcdir/diag.sh init
. $srcdir/diag.sh generate-conf
. $srcdir/diag.sh add-conf '
template(name="tpl" type="string"
	 string="{\"msgnum\":\"%msg:F,58:2%\"}")

module(load="../plugins/omelasticsearch/.libs/omelasticsearch")

if $msg contains "msgnum:" then
	action(type="omelasticsearch"
	       server="127.0.0.1"
	       serverport="19200"
	       template="tpl"
	       writeoperation="unknown"
	       searchIndex="rsyslog_testbench")

action(type="omfile" file="rsyslog.out.log")
'

# . $srcdir/diag.sh es-init
. $srcdir/diag.sh startup
. $srcdir/diag.sh injectmsg  0 1
. $srcdir/diag.sh shutdown-when-empty
. $srcdir/diag.sh wait-shutdown
if grep -q "omelasticsearch: invalid value 'unknown' for writeoperation" rsyslog.out.log ; then
	echo found correct error message
else
	echo Error: did not complain about incorrect writeoperation
	cat rsyslog.out.log
	. $srcdir/diag.sh error-exit 1
fi

. $srcdir/diag.sh init
. $srcdir/diag.sh generate-conf
. $srcdir/diag.sh add-conf '
template(name="tpl" type="string"
	 string="{\"msgnum\":\"%msg:F,58:2%\"}")

module(load="../plugins/omelasticsearch/.libs/omelasticsearch")

template(name="id-template" type="list") { constant(value="123456789") }

if $msg contains "msgnum:" then
	action(type="omelasticsearch"
	       server="127.0.0.1"
	       serverport="19200"
	       template="tpl"
	       writeoperation="create"
	       bulkid="id-template"
	       dynbulkid="on"
	       bulkmode="on"
	       searchIndex="rsyslog_testbench")

action(type="omfile" file="rsyslog.out.log")
'

export ES_PORT=19200
. $srcdir/diag.sh es-init
#export RSYSLOG_DEBUG="debug nostdout noprintmutexaction"
#export RSYSLOG_DEBUGLOG="debug.log"
. $srcdir/diag.sh startup
. $srcdir/diag.sh injectmsg  0 1
. $srcdir/diag.sh shutdown-when-empty
. $srcdir/diag.sh wait-shutdown
. $srcdir/diag.sh es-getdata 1 $ES_PORT

cat work | \
	python -c '
import sys,json
hsh = json.load(sys.stdin)
try:
	if hsh["hits"]["hits"][0]["_id"] == "123456789":
		print "good - found expected value"
		sys.exit(0)
	print "Error: _id not expected value 123456789:", hsh["hits"]["hits"][0]["_id"]
	sys.exit(1)
except ValueError:
	print "Error: output is not valid:", json.dumps(hsh,indent=2)
	sys.exit(1)
'

if [ $? -eq 0 ] ; then
	echo found correct response
else
	cat rsyslog.out.log
	. $srcdir/diag.sh error-exit 1
fi

. $srcdir/diag.sh stop-elasticsearch
. $srcdir/diag.sh cleanup-elasticsearch
. $srcdir/diag.sh exit
