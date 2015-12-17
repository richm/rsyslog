/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *
 * omamqp1.c
 *
 * This output plugin enables rsyslog to send messages to an AMQP 1.0 protocol
 * compliant message bus.
 *
 * AMQP glue code Copyright (C) 2015-2016 Kenneth A. Giusti
 * <kgiusti@gmail.com>
 */

#include "config.h"
#include "rsyslog.h"
#include "conf.h"
#include "syslogd-types.h"
#include "srUtils.h"
#include "template.h"
#include "module-template.h"
#include "errmsg.h"
#include "cfsysline.h"

#include <errno.h>
#include <pthread.h>
#include <time.h>

#include <proton/reactor.h>
#include <proton/handlers.h>
#include <proton/event.h>
#include <proton/connection.h>
#include <proton/session.h>
#include <proton/link.h>
#include <proton/delivery.h>
#include <proton/message.h>
#include <proton/transport.h>
#include <proton/sasl.h>


MODULE_TYPE_OUTPUT
MODULE_TYPE_NOKEEP
MODULE_CNFNAME("omamqp1")


/* internal structures
 */
DEF_OMOD_STATIC_DATA
DEFobjCurrIf(errmsg)

typedef struct _instanceData {
    int bIsRunning;     /* is I/O thread running? 0-no, 1-yes */
    int bDisableSASL;   /* do not enable SASL? 0-enable 1-disable */
    uchar *url;         /* address of message bus */
    uchar *username;
    uchar *password;
    uchar *target;
    uchar *templateName;
    //pn_connection_t *pn_connection;
    pthread_t thread_id;
} instanceData;


/* AMQP Glue Code */

static pn_timestamp_t _now()
{
    struct timespec ts = {0};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ts.tv_sec * (pn_timestamp_t)1000) + (ts.tv_nsec/(pn_timestamp_t)1000000);
}


const pn_state_t ENDPOINT_ACTIVE = (PN_LOCAL_ACTIVE | PN_REMOTE_ACTIVE);
const pn_state_t ENDPOINT_CLOSED = (PN_LOCAL_CLOSED | PN_REMOTE_CLOSED);
const pn_state_t ENDPOINT_OPENING = (PN_LOCAL_UNINIT | PN_REMOTE_ACTIVE);
const pn_state_t ENDPOINT_CLOSING = (PN_LOCAL_ACTIVE | PN_REMOTE_CLOSED);

/* state maintained by the protocol thread */
typedef struct {
    pn_reactor_t *reactor;  // AMQP 1.0 protocol engine
    pn_connection_t *conn;  // AMQP 1.0 connection
    pn_session_t *ssn;  // on connection
    pn_link_t *sender;  // on ssn
    int msgs_sent;
    int msgs_settled;
    char *encoded_buffer;
    size_t buffer_size;
    bool done;
} protocol_handler_t;


/* access the protocol_handler_t within the pn_handler_t */
static protocol_handler_t *_protocol_handler(pn_handler_t *handler)
{
  return (protocol_handler_t *) pn_handler_mem(handler);
}

typedef void dispatch_t(pn_handler_t *, pn_event_t *, pn_event_type_t);

/* create and initialize a protocol_handler_t instance */
static pn_handler_t *_new_handler(pn_reactor_t *reactor, dispatch_t *dispatcher)
{
  pn_handler_t *handler = pn_handler_new(dispatcher, sizeof(protocol_handler_t), NULL);
  protocol_handler_t *th = _protocol_handler(handler);
  th->msgs_sent = 0;
  th->msgs_settled = 0;
  th->done = false;
  th->buffer_size = 64;
  th->encoded_buffer = (char *)malloc(th->buffer_size);
  th->reactor = reactor;
  th->conn = NULL;
  th->ssn = NULL;
  th->sender = NULL;
  return handler;
}

/* release a protocol_handler_t instance */
static void _del_handler(pn_handler_t *handler)
{
  protocol_handler_t *th = _protocol_handler(handler);
  if (th->encoded_buffer) free (th->encoded_buffer);
  pn_decref(handler);
}



static void dispatcher(pn_handler_t *handler, pn_event_t *event, pn_event_type_t type)
{
    fprintf(stdout, "Event %s\n", pn_event_type_name(type));

    protocol_handler_t *ph = _protocol_handler(handler);
    switch (type) {
    case PN_CONNECTION_INIT:
        {
            pn_connection_t *conn = pn_event_connection(event);
            pn_connection_set_container(conn, "AContainerName");
            //pn_connection_set_hostname(conn, "amqp://localhost:5672");
            pn_connection_set_hostname(conn, "localhost:5672");
            // TODO: version detection??
            //pn_connection_set_user(conn, "guest");
            //pn_connection_set_password(conn, "guest");
            pn_connection_open(conn);
            fprintf(stdout, "CONNECTION REFCOUNT: %d\n", pn_refcount(ph->conn));
        }
        break;
    case PN_CONNECTION_REMOTE_OPEN:
    case PN_CONNECTION_LOCAL_OPEN:
        {
            // if local uninit call on_connection_opening  ?? auto_open
            // else call on_connection_opened
            pn_connection_t *conn = pn_event_connection(event);
            pn_state_t s = pn_connection_state(conn);
            if (s == ENDPOINT_ACTIVE) {
                fprintf(stdout, "CONN UP\n");
                pn_session_t *ssn = pn_session(conn);
                pn_session_open(ssn);
                pn_link_t *snd = pn_sender(ssn, "sender");
                pn_terminus_set_address(pn_link_target(snd), "amq.topic");
                pn_terminus_set_address(pn_link_source(snd), "amq.topic");
                pn_link_open(snd);
            } else {
                fprintf(stdout, "CONN STATE = 0x%X\n", s);
            }
        }
        break;
    case PN_LINK_REMOTE_OPEN:
    case PN_LINK_LOCAL_OPEN:
        {
            // if local open and remote open call on_link_opened
            // if remote open and not local call on_link_opening
            pn_link_t *snd = pn_event_link(event);
            pn_state_t s = pn_link_state(snd);
            if (s == ENDPOINT_ACTIVE) {
                fprintf(stdout, "LINK UP\n");
            }
            fprintf(stdout, "LINK REFCOUNT: %d\n", pn_refcount(snd));
        }
        break;

    case PN_LINK_FLOW:
        {

        static pn_timestamp_t lasttime = 0;
        pn_timestamp_t now = _now();
        if (lasttime && (now - lasttime) < 10000)
            break;
        lasttime = now;
        // if link is sender and credit > 0 call on_sendable
        static uint64_t tag = 0;

        pn_link_t *snd = pn_event_link(event);
        int credit = pn_link_credit(snd);
        fprintf(stdout, "Link credit = %d\n", credit);
        if (credit > 0) {
            ++tag;
            pn_delivery(snd, pn_dtag((const char *)&tag, sizeof(tag)));
            pn_message_t *msg = pn_message();
            pn_data_t *body = pn_message_body(msg);
            // TODO locking
            pn_data_put_string(body, pn_bytes(4, "WTF?"));
            int rc = 0;
            size_t len = 0;
            do {
                len = ph->buffer_size;
                rc = pn_message_encode(msg, ph->encoded_buffer, &len);
                if (rc == PN_OVERFLOW) {
                    ph->buffer_size *= 2;
                    free(ph->encoded_buffer);
                    ph->encoded_buffer = (char *)malloc(ph->buffer_size);
                }
            } while (rc == PN_OVERFLOW);
            pn_decref(msg);

            if (rc != 0) {
                fprintf(stderr, "HANDLE THIS\n");
                break;
            }
            pn_link_send(snd, ph->encoded_buffer, len);
            pn_link_advance(snd);
            ++ph->msgs_sent;
        }
    }
        break;
    case PN_DELIVERY: {
        // if sender and updated, call on_accepted or on_rejected or....
        //   and if remote settled call on_settled
        // if recver and readable not partial:
        //     call on_message, use return value to update delivery and settle
        // else if recvr and updated and settled call on_settled
        pn_link_t *snd = pn_event_link(event);
        if (!pn_link_is_sender(snd)) {
            fprintf(stderr, "RECEIVER???\n");
            break;
        }
        pn_delivery_t *dlv = pn_event_delivery(event);
        if (pn_delivery_updated(dlv)) {
            uint64_t state = pn_delivery_remote_state(dlv);
            fprintf(stdout, "remote delivery state = 0x%lX\n", state);
            if (state != PN_RECEIVED) {
                pn_delivery_settle(dlv);
            }
        }
    }
        break;
    case PN_LINK_LOCAL_CLOSE:
    case PN_LINK_REMOTE_CLOSE:
        {
            // if remote close check link->remote_condition and
            //   call on_link_error if needed
            // if local is closed, call on_link_closed
            // else call on_link_closing()
            /// ?? should we auto close??
            pn_link_t *snd = pn_event_link(event);
            pn_state_t s = pn_link_state(snd);
            if (s == ENDPOINT_CLOSED) {
                pn_session_t *ssn = pn_link_session(snd);
                pn_session_close(ssn);
                fprintf(stdout, "LINK REFCOUNT: %d\n", pn_refcount(snd));
            } else if (s == ENDPOINT_CLOSING) {
                // TODO error?
                pn_link_close(snd);
            }
        }
        break;

    case PN_SESSION_REMOTE_CLOSE:
    case PN_SESSION_LOCAL_CLOSE:
        {
            // see link
            pn_session_t *ssn = pn_event_session(event);
            pn_state_t s = pn_session_state(ssn);
            if (s == ENDPOINT_CLOSED) {
                pn_connection_t *conn = pn_session_connection(ssn);
                pn_connection_close(conn);
                fprintf(stdout, "SESSION REFCOUNT: %d\n", pn_refcount(ssn));
            } else if (s == ENDPOINT_CLOSING) {
                // TODO error?
                pn_session_close(ssn);
            }
        }
        break;

    case PN_CONNECTION_REMOTE_CLOSE:
    case PN_CONNECTION_LOCAL_CLOSE:
        {
            // see link
            pn_connection_t *conn = pn_event_connection(event);
            pn_state_t s = pn_connection_state(conn);
            if (s == ENDPOINT_CLOSING) {
                pn_connection_close(conn);
                fprintf(stdout, "CONNECTION REFCOUNT: %d\n", pn_refcount(conn));
            } else if (s == ENDPOINT_CLOSED) {
                ph->done = true;
            }
        }
        break;
    case PN_TRANSPORT_ERROR:
        {
            // log
            // if condition is fatal, close the connection
            pn_transport_t *tport = pn_event_transport(event);
            pn_condition_t *issue = pn_transport_condition(tport);
            pn_connection_t *conn = pn_event_connection(event);
            fprintf(stdout, "CONN REFCOUNT %d\n", pn_refcount(conn));
            fprintf(stdout, "TPORT REFCOUNT %d\n", pn_refcount(tport));

            if (pn_condition_is_set(issue)) {
                fprintf(stderr, "Transport Issue:\n"
                        "  name=%s\n"
                        "  description=%s\n",
                        pn_condition_get_name(issue),
                        pn_condition_get_description(issue));
            }
            pn_sasl_t *sasl = pn_sasl(tport);
            if (sasl && pn_sasl_outcome(sasl) == PN_SASL_AUTH) {
                fprintf(stderr, "Failed authentication\n");
            }
            ph->done = true;            
        }
        
        break;

    case PN_CONNECTION_BOUND:
        {
            pn_connection_t *conn = pn_event_connection(event);
            fprintf(stdout, "REFCOUNT %d\n", pn_refcount(conn));
        }
        break;

    case PN_CONNECTION_UNBOUND:
    {
        pn_connection_t *conn = pn_event_connection(event);
        fprintf(stdout, "REFCOUNT %d\n", pn_refcount(conn));
            /* pn_transport_t *tp = pn_event_transport(event); */
            /* if (!tp) fprintf(stdout, "WTF???"); */
            /* pn_transport_set_idle_timeout(tp, 10*1000); */
    }
    break;

        // transport_tail_closed
        // if connection and connection is locally opened, call on_disconnected

    case PN_TRANSPORT_CLOSED:
        {
            // log
            // if condition is fatal, close the connection
            pn_transport_t *tport = pn_event_transport(event);
            pn_connection_t *conn = pn_event_connection(event);
            if (conn) fprintf(stdout, "CONN REFCOUNT %d\n", pn_refcount(conn));
            fprintf(stdout, "TPORT REFCOUNT %d\n", pn_refcount(tport));
        }
        break;


    default:
        break;
    }

}


/* void *main(int argc, char **argv) */
/* { */
/*     pn_reactor_t *reactor = pn_reactor(); */
/*     //pn_handler_t *handler = pn_reactor_get_handler(reactor); */
/*     pn_handler_t *handler = new_handler(reactor); */
/*     test_handler_t *th = to_handler(handler); */
/*     //pn_handler_add(handler, th); */
/*     pn_reactor_connection(reactor, handler); */
/*     pn_reactor_start(reactor); */
/*     while (!th->done) { */
/*         fprintf(stdout, "Calling reactor run...\n"); */
/*         bool rc = pn_reactor_process(reactor); */
/*         fprintf(stdout, "... reactor returned %d\n", rc); */
/*         sleep(1); */
/*     } */
/*     pn_reactor_stop(reactor); */
/*     pn_reactor_free(reactor); */
/*     free(th->encoded_buffer); */
/*     pn_decref(handler); */
/*     return 0; */
/* } */


/* runs the protocol engine, allowing it to handle TCP socket I/O and timer
 * events when actions are not being serviced.
*/
static void *amqp1_thread(void *arg)
{
    pn_reactor_t *reactor = NULL;
    pn_handler_t *handler = NULL;
    protocol_handler_t *ph = NULL;
    bool stopped = false;

    while (!stopped) {
        // create the protocol engine (reactor)
        reactor = pn_reactor();
        handler = _new_handler(reactor, dispatcher);
        ph = _protocol_handler(handler);
        // setup a connection and run the engine
        ph->conn = pn_reactor_connection(reactor, handler);
        // TODO: is this necessary?:
        pn_reactor_set_timeout(reactor, 5000);
        pn_reactor_start(reactor);
        bool engine_running = true;
        while (engine_running) {
            engine_running = pn_reactor_process(reactor);
            // TODO: handle commands
            fprintf(stdout, "Reactor returned\n");
        }
        fprintf(stdout, "REACTOR DEAD? %d\n",
                (ph->conn) ? pn_refcount(ph->conn) : 0);
        fflush(NULL);
        ph->conn = NULL;
        pn_reactor_stop(reactor);
        _del_handler(handler);
        pn_reactor_free(reactor);

        // TODO reconnect backoff??

    }
    return 0;
}


static rsRetVal _launch_thread(instanceData *pData)
{
    int rc = pthread_create(&pData->thread_id, NULL, amqp1_thread, NULL);
    if (!rc) {
        pData->bIsRunning = true;
        return RS_RET_OK;
    }
    if (rc == EAGAIN) {
        DBGPRINTF("omamqp1: pthread_create returned EAGAIN, suspending\n");
        return RS_RET_SUSPENDED;
    }
    errmsg.LogError(0, RS_RET_SYS_ERR, "omamqp1: thread create failed: %d\n", rc);
    return RS_RET_SYS_ERR;
}

static rsRetVal _shutdown_thread(instanceData *pData)
{
    return RS_RET_OK;
    // TODO set flag, wake, and join
}


void KAG_trace(const char *m)
{
    fprintf(stdout, "KAG_TRACE: %s\n", m);
}







/* tables for interfacing with the v6 config system */
/* action (instance) parameters */
static struct cnfparamdescr actpdescr[] = {
    { "url", eCmdHdlrGetWord, CNFPARAM_REQUIRED },
    { "target", eCmdHdlrGetWord, CNFPARAM_REQUIRED },
    { "username", eCmdHdlrGetWord, 0 },
    { "password", eCmdHdlrGetWord, 0 },
    { "template", eCmdHdlrGetWord, 0 },
    { "disableSASL", eCmdHdlrInt, 0 }
};
static struct cnfparamblk actpblk = {
    CNFPARAMBLK_VERSION,
    sizeof(actpdescr)/sizeof(struct cnfparamdescr),
    actpdescr
};



//BEGINinitConfVars       /* (re)set config variables to default values */
//CODESTARTinitConfVars
//ENDinitConfVars


BEGINisCompatibleWithFeature
CODESTARTisCompatibleWithFeature
{
    if (eFeat == sFEATURERepeatedMsgReduction)
        iRet = RS_RET_OK;
}
ENDisCompatibleWithFeature


BEGINcreateInstance
CODESTARTcreateInstance
{
    KAG_trace("create instance");
}
ENDcreateInstance

// Set the default values for newly created Instance Data
static void setInstDefaults(instanceData __attribute__((unused)) *pData)
{
    memset(pData, 0, sizeof(*pData));
}


BEGINfreeInstance
CODESTARTfreeInstance
{
    _shutdown_thread(pData);
    if (pData->url) free(pData->url);
    if (pData->username) free(pData->username);
    if (pData->password) free(pData->password);
    if (pData->target) free(pData->target);
    if (pData->templateName) free(pData->templateName);
    //if (pData->pn_connection) pn_connection_free(pData->pn_connection);
}
ENDfreeInstance


BEGINdbgPrintInstInfo
CODESTARTdbgPrintInstInfo
{
    /* dump the instance data */
    dbgprintf("omamqp1\n");
    dbgprintf("  url=%s\n", pData->url);
    dbgprintf("  username=%s\n", pData->username);
    //dbgprintf("  password=%s", pData->password);
    dbgprintf("  target=%s\n", pData->target);
    dbgprintf("  template=%s\n", pData->templateName);
    dbgprintf("  disableSASL=%d\n", pData->bDisableSASL);
    dbgprintf("  running=%d\n", pData->bIsRunning);
}
ENDdbgPrintInstInfo


BEGINtryResume
CODESTARTtryResume
// KAG TODO
ENDtryResume


BEGINbeginTransaction
CODESTARTbeginTransaction
{
    KAG_trace("beginTransaction");
    if (!pData->bIsRunning) {
        iRet = _launch_thread(pData);
    }
}
ENDbeginTransaction


BEGINendTransaction
CODESTARTendTransaction
{
    KAG_trace("endTransaction");
}
finalize_it:
ENDendTransaction


BEGINdoAction
CODESTARTdoAction
{
    KAG_trace("doAction");
    fprintf(stdout, "LOG[%s]\n", ppString[0]);
    iRet = RS_RET_DEFER_COMMIT;
}
ENDdoAction




BEGINnewActInst
struct cnfparamvals *pvals;
int i;
CODESTARTnewActInst
{
    if ((pvals = nvlstGetParams(lst, &actpblk, NULL)) == NULL) {
        ABORT_FINALIZE(RS_RET_MISSING_CNFPARAMS);
    }

    CHKiRet(createInstance(&pData));
    setInstDefaults(pData);

    CODE_STD_STRING_REQUESTnewActInst(1);

    for(i = 0 ; i < actpblk.nParams ; ++i) {
        if (!pvals[i].bUsed)
            continue;
        if (!strcmp(actpblk.descr[i].name, "url")) {
            pData->url = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "template")) {
            pData->templateName = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "target")) {
            pData->target = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "username")) {
            pData->username = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "password")) {
            pData->password = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "disableSASL")) {
            pData->bDisableSASL = (int) pvals[i].val.d.n;
        } else {
            dbgprintf("omamqp1: program error, unrecognized param '%s', ignored.\n",
                      actpblk.descr[i].name);
        }
    }
#if 0
    if (pData->host == NULL) {
        errmsg.LogError(0, RS_RET_INVALID_PARAMS, "disabled: parameter host must be specified");
        ABORT_FINALIZE(RS_RET_INVALID_PARAMS);
    }
#endif

    CHKiRet(OMSRsetEntry(*ppOMSR,
                         0,
                         (uchar*)strdup((pData->templateName == NULL)
                                        ? "RSYSLOG_FileFormat"
                                        : (char*)pData->templateName),
                         OMSR_NO_RQD_TPL_OPTS));
}
CODE_STD_FINALIZERnewActInst
    cnfparamvalsDestruct(pvals, &actpblk);
ENDnewActInst


BEGINparseSelectorAct
CODESTARTparseSelectorAct
{
    CODE_STD_STRING_REQUESTparseSelectorAct(1);
    if (strncmp((char*) p, ":omamqp1:", sizeof(":omamqp1:") - 1)) {
        errmsg.LogError(0, RS_RET_LEGA_ACT_NOT_SUPPORTED,
                        "omamqp1 only supports the V6 configuration format."
                        " Example:\n"
                        " action(type=\"omamqp1.py\" url=<URL> target=<TARGET> ...)");
        ABORT_FINALIZE(RS_RET_CONFLINE_UNPROCESSED);
    }
}
CODE_STD_FINALIZERparseSelectorAct
ENDparseSelectorAct


BEGINmodExit
CODESTARTmodExit
    CHKiRet(objRelease(errmsg, CORE_COMPONENT));
finalize_it:
ENDmodExit


BEGINqueryEtryPt
CODESTARTqueryEtryPt
    CODEqueryEtryPt_STD_OMOD_QUERIES
    CODEqueryEtryPt_STD_CONF2_CNFNAME_QUERIES
    CODEqueryEtryPt_STD_CONF2_OMOD_QUERIES
    CODEqueryEtryPt_TXIF_OMOD_QUERIES /* we support the transactional interface! */
ENDqueryEtryPt


BEGINmodInit()
CODESTARTmodInit
{
    *ipIFVersProvided = CURR_MOD_IF_VERSION; /* we only support the current
                                                interface specification */
CODEmodInit_QueryRegCFSLineHdlr
    CHKiRet(objUse(errmsg, CORE_COMPONENT));
    INITChkCoreFeature(bCoreSupportsBatching, CORE_FEATURE_BATCHING);
    DBGPRINTF("omamqp1: module compiled with rsyslog version %s.\n", VERSION);
    DBGPRINTF("omamqp1: %susing transactional output interface.\n", bCoreSupportsBatching ? "" : "not ");
}
ENDmodInit

/* vi:set ai:
 */
