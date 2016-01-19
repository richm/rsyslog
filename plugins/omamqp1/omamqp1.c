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
#include <proton/version.h>


MODULE_TYPE_OUTPUT
MODULE_TYPE_NOKEEP
MODULE_CNFNAME("omamqp1")


/* internal structures
 */
DEF_OMOD_STATIC_DATA
DEFobjCurrIf(errmsg)


/* Settings for the action */
typedef struct _configSettings {
    uchar *host;         /* address of message bus */
    uchar *username;    /* authentication credentials */
    uchar *password;
    uchar *target;      /* endpoint for sent log messages */
    uchar *templateName;
    int bDisableSASL;   /* do not enable SASL? 0-enable 1-disable */
    int idleTimeout;    /* disconnect idle connection (seconds) */
    int retryDelay;     /* pause before re-connecting (seconds) */
} configSettings_t;


/* Control for communicating with the protocol engine thread */

typedef enum {          // commands sent to protocol thread
    COMMAND_DONE,       // marks command complete
    COMMAND_SEND,       // send a message to the message bus
    COMMAND_IS_READY,   // is the connection to the message bus active?
    COMMAND_SHUTDOWN    // cleanup and terminate protocol thread.
} commands_t;


typedef struct _threadIPC {
    pthread_mutex_t lock;
    pthread_cond_t condition;
    commands_t command;
    rsRetVal result;    // of command
    pn_message_t *message;
    uint64_t    tag;    // per message id
} threadIPC_t;


/* per-instance data */

typedef struct _instanceData {
    configSettings_t config;
    threadIPC_t ipc;
    int bThreadRunning;
    pthread_t thread_id;
    pn_reactor_t *reactor;
    pn_handler_t *handler;
    pn_message_t *message;
    int log_count;
} instanceData;


/* glue code */

typedef void dispatch_t(pn_handler_t *, pn_event_t *, pn_event_type_t);

static void _init_thread_ipc(threadIPC_t *pIPC);
static void _clean_thread_ipc(threadIPC_t *ipc);
static void _init_config_settings(configSettings_t *pConfig);
static void _clean_config_settings(configSettings_t *pConfig);
static rsRetVal _shutdown_thread(instanceData *pData);
static rsRetVal _new_handler(pn_handler_t **handler,
                             pn_reactor_t *reactor,
                             dispatch_t *dispatcher,
                             configSettings_t *config,
                             threadIPC_t *ipc);
static void _del_handler(pn_handler_t *handler);
static rsRetVal _launch_protocol_thread(instanceData *pData);
static rsRetVal _shutdown_thread(instanceData *pData);
static rsRetVal _issue_command(threadIPC_t *ipc,
                               pn_reactor_t *reactor,
                               commands_t command,
                               pn_message_t *message);
static void dispatcher(pn_handler_t *handler,
                       pn_event_t *event,
                       pn_event_type_t type);


/* tables for interfacing with the v6 config system */
/* action (instance) parameters */
static struct cnfparamdescr actpdescr[] = {
    { "host", eCmdHdlrGetWord, CNFPARAM_REQUIRED },
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
    memset(pData, 0, sizeof(instanceData));
    _init_config_settings(&pData->config);
    _init_thread_ipc(&pData->ipc);
}
ENDcreateInstance


BEGINfreeInstance
CODESTARTfreeInstance
{
    _shutdown_thread(pData);
    _clean_config_settings(&pData->config);
    _clean_thread_ipc(&pData->ipc);
    if (pData->reactor) pn_decref(pData->reactor);
    if (pData->handler) _del_handler(pData->handler);
    if (pData->message) pn_decref(pData->message);
}
ENDfreeInstance


BEGINdbgPrintInstInfo
CODESTARTdbgPrintInstInfo
{
#if 0

    /* TODO: dump the instance data */
    dbgprintf("omamqp1\n");
    dbgprintf("  host=%s\n", pData->host);
    dbgprintf("  username=%s\n", pData->username);
    //dbgprintf("  password=%s", pData->password);
    dbgprintf("  target=%s\n", pData->target);
    dbgprintf("  template=%s\n", pData->templateName);
    dbgprintf("  disableSASL=%d\n", pData->bDisableSASL);
    dbgprintf("  running=%d\n", pData->bIsRunning);
#endif
}
ENDdbgPrintInstInfo


BEGINtryResume
CODESTARTtryResume
{
    // is the link active?
    iRet = _issue_command(&pData->ipc, pData->reactor, COMMAND_IS_READY, NULL);
}
ENDtryResume


BEGINbeginTransaction
CODESTARTbeginTransaction
{
    pData->log_count = 0;
    if (pData->message) pn_decref(pData->message);
    pData->message = pn_message();
    CHKmalloc(pData->message);
    pn_data_t *body = pn_message_body(pData->message);
    pn_data_put_list(body);
    pn_data_enter(body);
}
finalize_it:
ENDbeginTransaction


BEGINdoAction
CODESTARTdoAction
{
    pn_bytes_t msg = pn_bytes(strlen((const char *)ppString[0]),
                              (const char *)ppString[0]);
    assert(pData->message);
    pn_data_t *body = pn_message_body(pData->message);
    pn_data_put_string(body, msg);
    pData->log_count++;
    iRet = RS_RET_DEFER_COMMIT;
}
ENDdoAction


BEGINendTransaction
CODESTARTendTransaction
{
    assert(pData->message);
    pn_data_t *body = pn_message_body(pData->message);
    pn_data_exit(body);
    pn_message_t *message = pData->message;
    pData->message = NULL;
    if (pData->log_count > 0) {
        CHKiRet(_issue_command(&pData->ipc, pData->reactor, COMMAND_SEND, message));
    } else {
        DBGPRINTF("omamqp1: no log messages to send\n");
        pn_decref(message);
    }
}
finalize_it:
ENDendTransaction


BEGINnewActInst
struct cnfparamvals *pvals;
int i;
configSettings_t *cs;
CODESTARTnewActInst
{
    if ((pvals = nvlstGetParams(lst, &actpblk, NULL)) == NULL) {
        ABORT_FINALIZE(RS_RET_MISSING_CNFPARAMS);
    }

    CHKiRet(createInstance(&pData));
    cs = &pData->config;

    CODE_STD_STRING_REQUESTnewActInst(1);

    for(i = 0 ; i < actpblk.nParams ; ++i) {
        if (!pvals[i].bUsed)
            continue;
        if (!strcmp(actpblk.descr[i].name, "host")) {
            cs->host = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "template")) {
            cs->templateName = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "target")) {
            cs->target = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "username")) {
            cs->username = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "password")) {
            cs->password = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "disableSASL")) {
            cs->bDisableSASL = (int) pvals[i].val.d.n;
        } else {
            // TODO retrydelay, idle timeout
            dbgprintf("omamqp1: program error, unrecognized param '%s', ignored.\n",
                      actpblk.descr[i].name);
        }
    }

    CHKiRet(OMSRsetEntry(*ppOMSR,
                         0,
                         (uchar*)strdup((cs->templateName == NULL)
                                        ? "RSYSLOG_FileFormat"
                                        : (char*)cs->templateName),
                         OMSR_NO_RQD_TPL_OPTS));

    // once configuration is known, start the protocol engine thread
    pData->reactor = pn_reactor();
    CHKmalloc(pData->reactor);
    CHKiRet(_new_handler(&pData->handler, pData->reactor, dispatcher, &pData->config, &pData->ipc));
    CHKiRet(_launch_protocol_thread(pData));
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
                        " action(type=\"omamqp1.py\" host=<address[:port]> target=<TARGET> ...)");
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
    CODEqueryEtryPt_TXIF_OMOD_QUERIES   /* use transaction interface */
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


///////////////////////////////////////
// All the Proton-specific glue code //
///////////////////////////////////////


/* state maintained by the protocol thread */

typedef struct {
    const configSettings_t *config;
    threadIPC_t   *ipc;
    pn_reactor_t *reactor;  // AMQP 1.0 protocol engine
    pn_connection_t *conn;
    pn_link_t *sender;
    pn_delivery_t *delivery;
    char *encode_buffer;
    size_t buffer_size;
    uint64_t tag;
    int msgs_sent;
    int msgs_settled;
    sbool stopped;
} protocolState_t;

// protocolState_t is embedded in the engine handler
#define PROTOCOL_STATE(eh) ((protocolState_t *) pn_handler_mem(eh))


static void _init_config_settings(configSettings_t *pConfig)
{
    memset(pConfig, 0, sizeof(configSettings_t));
    pConfig->idleTimeout = 30;
    pConfig->retryDelay = 5;
}


static void _clean_config_settings(configSettings_t *pConfig)
{
    if (pConfig->host) free(pConfig->host);
    if (pConfig->username) free(pConfig->username);
    if (pConfig->password) free(pConfig->password);
    if (pConfig->target) free(pConfig->target);
    if (pConfig->templateName) free(pConfig->templateName);
    memset(pConfig, 0, sizeof(configSettings_t));
}


static void _init_thread_ipc(threadIPC_t *pIPC)
{
    memset(pIPC, 0, sizeof(threadIPC_t));
    pthread_mutex_init(&pIPC->lock, NULL);
    pthread_cond_init(&pIPC->condition, NULL);
    pIPC->command = COMMAND_DONE;
    pIPC->result = RS_RET_OK;
}

static void _clean_thread_ipc(threadIPC_t *ipc)
{
    pthread_cond_destroy(&ipc->condition);
    pthread_mutex_destroy(&ipc->lock);
}


// create a new handler for the engine and set up the protocolState
static rsRetVal _new_handler(pn_handler_t **handler,
                             pn_reactor_t *reactor,
                             dispatch_t *dispatch,
                             configSettings_t *config,
                             threadIPC_t *ipc)
{
    DEFiRet;
    *handler = pn_handler_new(dispatch, sizeof(protocolState_t), NULL);
    CHKmalloc(*handler);
    protocolState_t *pState = PROTOCOL_STATE(*handler);
    memset(pState, 0, sizeof(protocolState_t));
    pState->buffer_size = 64;  // will grow if not enough
    pState->encode_buffer = (char *)malloc(pState->buffer_size);
    CHKmalloc(pState->encode_buffer);
    pState->reactor = reactor;
    pn_incref(reactor);
    pState->stopped = false;
    // these are _references_, don't free them:
    pState->config = config;
    pState->ipc = ipc;

 finalize_it:
    RETiRet;
}


// in case existing buffer too small
static rsRetVal _grow_buffer(protocolState_t *pState)
{
    DEFiRet;
    pState->buffer_size *= 2;
    free(pState->encode_buffer);
    pState->encode_buffer = (char *)malloc(pState->buffer_size);
    CHKmalloc(pState->encode_buffer);

 finalize_it:
    RETiRet;
}


/* release the pn_handler_t instance */
static void _del_handler(pn_handler_t *handler)
{
    protocolState_t *pState = PROTOCOL_STATE(handler);
    if (pState->encode_buffer) free(pState->encode_buffer);
    if (pState->delivery) pn_delivery_settle(pState->delivery);
    if (pState->sender) pn_decref(pState->sender);
    if (pState->conn) pn_decref(pState->conn);
    if (pState->reactor) pn_decref(pState->reactor);
    pn_decref(handler);
}


// abort any pending send requests.  This is done if the connection to the
// message bus drops unexpectantly.
static void _abort_send_command(protocolState_t *ps)
{
    threadIPC_t *ipc = ps->ipc;

    pthread_mutex_lock(&ipc->lock);
    if (ipc->command == COMMAND_SEND) {
        if (ps->delivery) {
            errmsg.LogError(0, NO_ERRCODE,
                            "omamqp1: aborted the message send in progress");
            pn_delivery_settle(ps->delivery);
            ps->delivery = NULL;
        }
        ipc->result = RS_RET_SUSPENDED;
        ipc->command = COMMAND_DONE;
        pthread_cond_signal(&ipc->condition);
    }
    pthread_mutex_unlock(&ipc->lock);
}


// log a protocol error received from the message bus
static void _log_error(const char *message, pn_condition_t *cond)
{
    const char *name = pn_condition_get_name(cond);
    const char *desc = pn_condition_get_description(cond);
    errmsg.LogError(0, NO_ERRCODE,
                    "omamqp1: %s %s:%s\n",
                    message,
                    (name) ? name : "<no name>",
                    (desc) ? desc : "<no description>");
}

// link, session, connection endpoint state flags
static const pn_state_t ENDPOINT_ACTIVE = (PN_LOCAL_ACTIVE | PN_REMOTE_ACTIVE);
static const pn_state_t ENDPOINT_CLOSING = (PN_LOCAL_ACTIVE | PN_REMOTE_CLOSED);
static const pn_state_t ENDPOINT_CLOSED = (PN_LOCAL_CLOSED | PN_REMOTE_CLOSED);


/* is the link ready to send messages? */
static sbool _is_ready(pn_link_t *link)
{
    return (link
            && pn_link_state(link) == ENDPOINT_ACTIVE
            && pn_link_credit(link) > 0);
}


/* Process each event emitted by the protocol engine */
static void dispatcher(pn_handler_t *handler, pn_event_t *event, pn_event_type_t type)
{
    protocolState_t *ps = PROTOCOL_STATE(handler);

    //DBGPRINTF("omamqp1: Event received: %s\n", pn_event_type_name(type));

    switch (type) {

    case PN_LINK_REMOTE_OPEN:
        DBGPRINTF("omamqp1: Message bus opened link.\n");
        break;

    case PN_DELIVERY:
        // has the message been delivered to the message bus?
        if (ps->delivery) {
            assert(ps->delivery == pn_event_delivery(event));
            if (pn_delivery_updated(ps->delivery)) {
                rsRetVal result = RS_RET_IDLE;
                uint64_t rs = pn_delivery_remote_state(ps->delivery);
                switch (rs) {
                case PN_ACCEPTED:
                    DBGPRINTF("omamqp1: Message ACCEPTED by message bus\n");
                    result = RS_RET_OK;
                    break;
                case PN_REJECTED:
                    errmsg.LogError(0, NO_ERRCODE,
                                    "omamqp1: peer rejected log message, dropping");
                    result = RS_RET_ERR;
                    break;
                case PN_RELEASED:
                    DBGPRINTF("omamqp1: peer unable to accept message, suspending");
                    result = RS_RET_SUSPENDED;
                    break;
                case PN_MODIFIED:
                    if (pn_disposition_is_undeliverable(pn_delivery_remote(ps->delivery))) {
                        errmsg.LogError(0, NO_ERRCODE,
                                        "omamqp1: log message undeliverable, dropping");
                        result = RS_RET_ERR;
                    } else {
                        DBGPRINTF("omamqp1: message modified, suspending");
                        result = RS_RET_SUSPENDED;
                    }
                    break;
                case PN_RECEIVED:
                    // not finished yet, wait for next delivery update
                    break;
                default:
                    // no other terminal states defined, so ignore anything else
                    errmsg.LogError(0, NO_ERRCODE,
                                    "omamqp1: unknown delivery state=0x%lX, ignoring",
                                    (unsigned long) pn_delivery_remote_state(ps->delivery));
                    break;
                }

                if (result != RS_RET_IDLE) {
                    // the command is complete
                    threadIPC_t *ipc = ps->ipc;
                    pthread_mutex_lock(&ipc->lock);
                    assert(ipc->command == COMMAND_SEND);
                    ipc->result = result;
                    ipc->command = COMMAND_DONE;
                    pthread_cond_signal(&ipc->condition);
                    pthread_mutex_unlock(&ipc->lock);
                    pn_delivery_settle(ps->delivery);
                    ps->delivery = NULL;
                    if (result == RS_RET_ERR) {
                        // try reconnecting to clear the error
                        if (ps->sender) pn_link_close(ps->sender);
                    }
                }
            }
        }
        break;

    case PN_LINK_REMOTE_CLOSE:
    case PN_LINK_LOCAL_CLOSE:
        if (ps->sender) {
            assert(ps->sender == pn_event_link(event));
            pn_state_t ls = pn_link_state(ps->sender);
            if (ls == ENDPOINT_CLOSING) {
                DBGPRINTF("omamqp1: remote closed the link\n");
                // check if remote signalled an error:
                pn_condition_t *cond = pn_link_condition(ps->sender);
                if (pn_condition_is_set(cond)) {
                    _log_error("link failure", cond);
                    // no recovery - reset the connection
                    if (ps->conn) pn_connection_close(ps->conn);
                } else {
                    pn_link_close(ps->sender);
                }
            } else if (ls == ENDPOINT_CLOSED) { // done
                DBGPRINTF("omamqp1: link closed\n");
                // close parent:
                pn_session_close(pn_link_session(ps->sender));
            }
        }
        break;

    case PN_SESSION_REMOTE_CLOSE:
    case PN_SESSION_LOCAL_CLOSE:
        {
            pn_session_t *session = pn_event_session(event);
            pn_state_t ss = pn_session_state(session);
            if (ss == ENDPOINT_CLOSING) {
                DBGPRINTF("omamqp1: remote closed the session\n");
                // check if remote signalled an error:
                pn_condition_t *cond = pn_session_condition(session);
                if (pn_condition_is_set(cond)) {
                    _log_error("session failure", cond);
                    // no recovery - reset the connection
                    if (ps->conn) pn_connection_close(ps->conn);
                } else {
                    pn_session_close(session);
                }
            } else if (ss == ENDPOINT_CLOSED) { // done
                // close parent:
                DBGPRINTF("omamqp1: session closed\n");
                if (ps->conn) pn_connection_close(ps->conn);
            }
        }
        break;

    case PN_CONNECTION_REMOTE_CLOSE:
    case PN_CONNECTION_LOCAL_CLOSE:
        if (ps->conn) {
            assert(ps->conn == pn_event_connection(event));
            pn_state_t cs = pn_connection_state(ps->conn);
            if (cs == ENDPOINT_CLOSING) {  // remote initiated close
                DBGPRINTF("omamqp1: remote closed the connection\n");
                // check if remote signalled an error:
                pn_condition_t *cond = pn_connection_condition(ps->conn);
                if (pn_condition_is_set(cond)) {
                    _log_error("connection failure", cond);
                }
                pn_connection_close(ps->conn);
            } else if (cs == ENDPOINT_CLOSED) {
                DBGPRINTF("omamqp1: connection closed\n");
                // the protocol thread will attempt to reconnect if it is not
                // being shut down
            }
        }
        break;

    case PN_TRANSPORT_ERROR:
        {
            // TODO: if auth failure, does it make sense to retry???
            pn_transport_t *tport = pn_event_transport(event);
            pn_condition_t *cond = pn_transport_condition(tport);
            if (pn_condition_is_set(cond)) {
                _log_error("transport failure", cond);
            }
            errmsg.LogError(0, NO_ERRCODE,
                            "omamqp1: network transport failed, reconnecting...");
            // the protocol thread will attempt to reconnect if it is not
            // being shut down
        }
        break;

    default:
        break;
    }
}


// Send a command to the protocol thread and
// wait for the command to complete
static rsRetVal _issue_command(threadIPC_t *ipc,
                               pn_reactor_t *reactor,
                               commands_t command,
                               pn_message_t *message)
{
    DEFiRet;

    DBGPRINTF("omamqp1: Sending command %d to protocol thread\n", command);

    pthread_mutex_lock(&ipc->lock);

    if (message) {
        assert(ipc->message == NULL);
        ipc->message = message;
    }
    assert(ipc->command == COMMAND_DONE);
    ipc->command = command;
    pn_reactor_wakeup(reactor);
    while (ipc->command != COMMAND_DONE) {
        pthread_cond_wait(&ipc->condition, &ipc->lock);
    }
    iRet = ipc->result;
    if (ipc->message) {
        pn_decref(ipc->message);
        ipc->message = NULL;
    }

    pthread_mutex_unlock(&ipc->lock);

    DBGPRINTF("omamqp1: Command %d completed, status=%d\n", command, iRet);
    RETiRet;
}


// check if a command needs processing
static void _poll_command(protocolState_t *ps)
{
    if (ps->stopped) return;

    threadIPC_t *ipc = ps->ipc;

    pthread_mutex_lock(&ipc->lock);

    switch (ipc->command) {

    case COMMAND_SHUTDOWN:
        DBGPRINTF("omamqp1: Protocol thread processing shutdown command\n");
        ps->stopped = true;
        if (ps->sender) pn_link_close(ps->sender);
        // wait for the shutdown to complete before ack'ing this command
        break;

    case COMMAND_IS_READY:
        DBGPRINTF("omamqp1: Protocol thread processing ready query command\n");
        ipc->result = _is_ready(ps->sender)
                      ? RS_RET_OK
                      : RS_RET_SUSPENDED;
        ipc->command = COMMAND_DONE;
        pthread_cond_signal(&ipc->condition);
        break;

    case COMMAND_SEND:
        if (ps->delivery) break;  // currently processing this command
        DBGPRINTF("omamqp1: Protocol thread processing send message command\n");
        if (!_is_ready(ps->sender)) {
            ipc->result = RS_RET_SUSPENDED;
            ipc->command = COMMAND_DONE;
            pthread_cond_signal(&ipc->condition);
            break;
        }

        // send the message
        ++ps->tag;
        ps->delivery = pn_delivery(ps->sender,
                                   pn_dtag((const char *)&ps->tag, sizeof(ps->tag)));
        pn_message_t *message = ipc->message;
        assert(message);

        int rc = 0;
        size_t len = ps->buffer_size;
        do {
            rc = pn_message_encode(message, ps->encode_buffer, &len);
            if (rc == PN_OVERFLOW) {
                _grow_buffer(ps);
                len = ps->buffer_size;
            }
        } while (rc == PN_OVERFLOW);

        pn_link_send(ps->sender, ps->encode_buffer, len);
        pn_link_advance(ps->sender);
        ++ps->msgs_sent;
        // command completes when remote updates the delivery (see PN_DELIVERY)
        break;

    case COMMAND_DONE:
        break;
    }

    pthread_mutex_unlock(&ipc->lock);
}

/* runs the protocol engine, allowing it to handle TCP socket I/O and timer
 * events in the background.
*/
static void *amqp1_thread(void *arg)
{
    DBGPRINTF("omamqp1: Protocol thread started\n");

    pn_handler_t *handler = (pn_handler_t *)arg;
    protocolState_t *ps = PROTOCOL_STATE(handler);
    const configSettings_t *cfg = ps->config;

    // TODO: timeout necessary??
    pn_reactor_set_timeout(ps->reactor, 5000);
    pn_reactor_start(ps->reactor);

    while (!ps->stopped) {
        // setup a connection:
        ps->conn = pn_reactor_connection(ps->reactor, handler);
        pn_incref(ps->conn);
        pn_connection_set_container(ps->conn, "rsyslogd-omamqp1");
        pn_connection_set_hostname(ps->conn, (cfg->host
                                              ? (char *)cfg->host
                                              : "localhost:5672"));

#if PN_VERSION_MAJOR == 0 && PN_VERSION_MINOR >= 10
        // proton version <= 0.9 did not support Cyrus SASL
        if (cfg->username && cfg->password) {
            pn_connection_set_user(ps->conn, cfg->username);
            pn_connection_set_password(ps->conn, cfg->password);
        }
#endif
        pn_connection_open(ps->conn);
        pn_session_t *ssn = pn_session(ps->conn);
        pn_session_open(ssn);
        ps->sender = pn_sender(ssn, (cfg->target
                                     ? (char *) cfg->target
                                     : "rsyslogd-omamqp1"));
        pn_link_set_snd_settle_mode(ps->sender, PN_SND_UNSETTLED);
        char *addr = (char *)ps->config->target;
        pn_terminus_set_address(pn_link_target(ps->sender), addr);
        pn_terminus_set_address(pn_link_source(ps->sender), addr);
        pn_link_open(ps->sender);

        // run the protocol engine until the connection closes or thread is shut down
        sbool engine_running = true;
        while (engine_running) {
            engine_running = pn_reactor_process(ps->reactor);
            _poll_command(ps);
        }

        DBGPRINTF("omamqp1: reactor finished\n");

        _abort_send_command(ps);   // if the connection dropped while sending
        pn_decref(ps->sender);
        ps->sender = NULL;
        pn_decref(ps->conn);
        ps->conn = NULL;

        // delay retryDelay seconds before re-connecting:
        int delay = ps->config->retryDelay;
        while (delay-- > 0 && !ps->stopped) {
            srSleep(1, 0);
            _poll_command(ps);
        }
    }
    pn_reactor_stop(ps->reactor);

    // stop command is now done:
    threadIPC_t *ipc = ps->ipc;
    pthread_mutex_lock(&ipc->lock);
    ipc->result = RS_RET_OK;
    ipc->command = COMMAND_DONE;
    pthread_cond_signal(&ipc->condition);
    pthread_mutex_unlock(&ipc->lock);

    DBGPRINTF("omamqp1: Protocol thread stopped\n");

    return 0;
}


static rsRetVal _launch_protocol_thread(instanceData *pData)
{
    int rc;
    DBGPRINTF("omamqp1: Starting protocol thread\n");
    do {
        rc = pthread_create(&pData->thread_id, NULL, amqp1_thread, pData->handler);
        if (!rc) {
            pData->bThreadRunning = true;
            return RS_RET_OK;
        }
    } while (rc == EAGAIN);
    errmsg.LogError(0, RS_RET_SYS_ERR, "omamqp1: thread create failed: %d\n", rc);
    return RS_RET_SYS_ERR;
}

static rsRetVal _shutdown_thread(instanceData *pData)
{
    DEFiRet;

    if (pData->bThreadRunning) {
        DBGPRINTF("omamqp1: shutting down thread...\n");
        CHKiRet(_issue_command(&pData->ipc, pData->reactor, COMMAND_SHUTDOWN, NULL));
        pthread_join(pData->thread_id, NULL);
        pData->bThreadRunning = false;
        DBGPRINTF("omamqp1: thread shutdown complete\n");
    }

 finalize_it:
    RETiRet;
}



/* vi:set ai:
 */

