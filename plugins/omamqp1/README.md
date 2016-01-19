# AMQP 1.0 Output Module #

The omamqp1 output module can be used to send log messages via an AMQP
1.0-compatible messaging bus.

This plugin requires the Apache QPID Proton python library, version
0.10+.  This should be installed on the system that is running
rsyslogd.

## Message Format ##

Messages sent from this module to the message bus contain a list of
strings.  Each string is a separate log message.  The list is ordered
such that the oldest log appears at the front of the list, whilst the
most recent log is at the end of the list.

## Configuration ##

This module is configured via the rsyslog.conf configuration file.  To
use this module it must first be imported.

Example:

   module(load="omamqp1")

Actions can then be created using this module.

Example:

    action(type="omamqp1"
           template="RSYSLOG_TraditionalFileFormat"
           host="localhost:5672"
           target="amq.topic")

The following parameters are recognized by the plugin:

* template - Template used by the action.
* host - The address of the message bus.  Optionally a port can be included, separated by a ':'.  Default: "localhost:5672"
* target - The destination for the generated messages.  This can be the name of a queue or topic.  Default: "rsyslog-omamqp1"
* username - Optional.  Used by SASL to authenticate with the message bus.
* password - Optional.  Used by SASL to authenticate with the message bus.
