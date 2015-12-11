/* omamqp1.c
 * This output plugin enables rsyslog to execute a program and
 * feed it the message stream as standard input.
 *
 * NOTE: read comments in module-template.h for more specifics!
 *
 * File begun on 2009-04-01 by RGerhards
 *
 * Copyright 2009-2012 Adiscon GmbH.
 *
 * This file is part of rsyslog.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 *       -or-
 *       see COPYING.ASL20 in the source distribution
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
} instanceData;

static void setInstDefaults(instanceData* data)
{
    memset(data, 0, sizeof(*data));
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

/* config settings */

BEGINcreateInstance
CODESTARTcreateInstance
ENDcreateInstance


BEGINisCompatibleWithFeature
CODESTARTisCompatibleWithFeature
{
    if (eFeat == sFEATURERepeatedMsgReduction)
        iRet = RS_RET_OK;
}
ENDisCompatibleWithFeature


BEGINfreeInstance
CODESTARTfreeInstance
{
    if (pData->url) free(pData->url);
    if (pData->username) free(pData->username);
    if (pData->password) free(pData->password);
    if (pData->target) free(pData->target);
    if (pData->templateName) free(pData->templateName);
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



BEGINdoAction
//pn_connection_t *conn;
CODESTARTdoAction
{
    //    conn = pn_connection();
    //    pn_connection_free(conn);
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
        errmsg.LogError(0, RS_RET_INVALID_PARAMS, "omrabbitmq module disabled: parameter host must be specified");
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
ENDqueryEtryPt



BEGINmodInit()
CODESTARTmodInit
{
    *ipIFVersProvided = CURR_MOD_IF_VERSION; /* we only support the current
                                                interface specification */
CODEmodInit_QueryRegCFSLineHdlr
    CHKiRet(objUse(errmsg, CORE_COMPONENT));
}
ENDmodInit

/* vi:set ai:
 */
