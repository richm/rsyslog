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
curdir=.

. $srcdir/diag.sh generate-conf
. $srcdir/diag.sh add-conf '
# comment out if you need more debug info:
	global( debug.whitelist="on"
		debug.files=["imfile.c"])

module(load="../plugins/imfile/.libs/imfile"
       mode="inotify" normalizePath="off")

input(type="imfile" File="'${curdir}'/rsyslog.input-symlink.log" Tag="file:"
	Severity="error" Facility="local7" addMetadata="on")

input(type="imfile" File="'${curdir}'/rsyslog.input.*.log" Tag="file:"
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
if [ "x${USE_VALGRIND:-false}" = "xtrue" ] ; then
	. $srcdir/diag.sh startup-vg
else
	. $srcdir/diag.sh startup
fi
if [ -n "${USE_GDB:-}" ] ; then
  echo attach gdb here
  sleep 54321 || :
fi


for i in `seq 2 $IMFILEINPUTFILES`;
do
	cp $imfilebefore targets/$i.log
	ln -s targets/$i.log rsyslog-link.$i.log
	ln -s rsyslog-link.$i.log rsyslog.input.$i.log
	imfilebefore="targets/$i.log"
	# Wait little for correct timing
	./msleep 50
done
./inputfilegen -m 3 > rsyslog.input.$((IMFILEINPUTFILES + 1)).log
ls -l rsyslog.input.* rsyslog-link.* targets

. $srcdir/diag.sh shutdown-when-empty # shut down rsyslogd when done processing messages
if [ "x${USE_VALGRIND:-false}" = "xtrue" ] ; then
	. $srcdir/diag.sh wait-shutdown-vg
	. $srcdir/diag.sh check-exit-vg
else
	. $srcdir/diag.sh wait-shutdown
fi

sort rsyslog.out.log > rsyslog.out.sorted.log

{
	echo HEADER msgnum:00000000:, filename: ${curdir}/rsyslog.input-symlink.log, fileoffset: 0
	for i in `seq 2 $IMFILEINPUTFILES` ; do
		echo HEADER msgnum:00000000:, filename: ${curdir}/rsyslog.input.${i}.log, fileoffset: 0
	done
	echo HEADER msgnum:00000000:, filename: ${curdir}/rsyslog.input.11.log, fileoffset: 0
	echo HEADER msgnum:00000001:, filename: ${curdir}/rsyslog.input.11.log, fileoffset: 17
	echo HEADER msgnum:00000002:, filename: ${curdir}/rsyslog.input.11.log, fileoffset: 34
} | sort | cmp - rsyslog.out.sorted.log
if [ ! $? -eq 0 ]; then
  echo "invalid output generated, rsyslog.out.log is:"
  cat rsyslog.out.log
  exit 1
fi;

. $srcdir/diag.sh exit
