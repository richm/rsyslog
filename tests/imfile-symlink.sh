#!/bin/bash
# This is part of the rsyslog testbench, licensed under GPLv3
export IMFILEINPUTFILES="10"
echo [imfile-symlink.sh]
. $srcdir/diag.sh check-inotify
. $srcdir/diag.sh init
# generate input files first. Note that rsyslog processes it as
# soon as it start up (so the file should exist at that point).

imfilebefore="rsyslog.input-symlink.log"
./inputfilegen -m 1 > $imfilebefore
mkdir targets

. $srcdir/diag.sh generate-conf
. $srcdir/diag.sh add-conf '
# comment out if you need more debug info:
	global( debug.whitelist="on"
		debug.files=["imfile.c"])

module(load="../plugins/imfile/.libs/imfile"
       mode="inotify" normalizePath="off")

input(type="imfile" File="./rsyslog.input-symlink.log" Tag="file:"
	Severity="error" Facility="local7" addMetadata="on")

input(type="imfile" File="./rsyslog.input.*.log" Tag="file:"
	Severity="error" Facility="local7" addMetadata="on")

template(name="outfmt" type="list") {
	constant(value="HEADER ")
	property(name="msg" format="json")
	constant(value=", filename: ")
	property(name="$!metadata!filename")
	constant(value=", fileoffset: ")
	property(name="$!metadata!fileoffset")
	constant(value="\n")
}

if $msg contains "msgnum:" then
	action( type="omfile" file="rsyslog.out.log" template="outfmt")
'
# Start rsyslog now before adding more files
. $srcdir/diag.sh startup

for i in `seq 2 $IMFILEINPUTFILES`;
do
	cp $imfilebefore targets/rsyslog.input.$i.log
	ln -s targets/$i.log rsyslog.input.$i.log
	imfilebefore="rsyslog.input.$i.log"
	# Wait little for correct timing
	./msleep 50
done
./inputfilegen -m 3 > rsyslog.input.$((IMFILEINPUTFILES + 1)).log
ls -l rsyslog.input.*

. $srcdir/diag.sh shutdown-when-empty # shut down rsyslogd when done processing messages
. $srcdir/diag.sh wait-shutdown	# we need to wait until rsyslogd is finished!

printf 'HEADER msgnum:00000000:, filename: ./rsyslog.input-symlink.log, fileoffset: 0
HEADER msgnum:00000000:, filename: ./targets/rsyslog.input.0.log, fileoffset: 0
HEADER msgnum:00000000:, filename: ./targets/rsyslog.input.1.log, fileoffset: 0' | cmp - rsyslog.out.log
if [ ! $? -eq 0 ]; then
  echo "invalid output generated, rsyslog.out.log is:"
  cat rsyslog.out.log
  exit 1
fi;

. $srcdir/diag.sh exit
