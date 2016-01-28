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

* host - The address of the message bus.  Optionally a port can be
  included, separated by a ':'.  Example: "localhost:5672"
* target - The destination for the generated messages.  This can be
  the name of a queue or topic.  On some messages buses it may be
  necessary to create this target manually.  Example: "amq.topic"
* username - Optional.  Used by SASL to authenticate with the message bus.
* password - Optional.  Used by SASL to authenticate with the message bus.
* template - Template used by the action.
* idleTimeout - The idle timeout in seconds.  This enables connection
  heartbeats and is used to detect a failed connection to the message
  bus.  Set to zero to disable.
* retryDelay - The time in seconds this plugin will delay before
  attempting to re-established a failed connection (default 5
  seconds).
* disableSASL - Setting this to a non-zero value will disable SASL
  negotiation.  Only necessary if the message bus does not offer SASL.
