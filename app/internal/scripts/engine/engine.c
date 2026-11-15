/* Built only by build-engine.py, never by cgo. No quickjs-libc or module loader. */
#include "quickjs.h"
#include <stdlib.h>
#include <string.h>
static JSRuntime *rt;
static JSContext *ctx;
static JSValue control;
static const char *output;
static JSValue invoke(const char *name, int argc, JSValue *args) {
    JSValue fn=JS_GetPropertyStr(ctx, control, name);
    JSValue value=JS_Call(ctx,fn,control,argc,args);
    JS_FreeValue(ctx,fn);
    return value;
}
static int save(JSValue value) {
    if (output) JS_FreeCString(ctx,output);
    if(JS_IsException(value)) { JSValue e=JS_GetException(ctx); output=JS_ToCString(ctx,e); JS_FreeValue(ctx,e); return -1; }
    output=JS_ToCString(ctx,value); JS_FreeValue(ctx,value);
    return output ? (int)strlen(output) : -1;
}
int initialize(const char *source, int length) {
    rt=JS_NewRuntime(); if(!rt)return -1;
    JS_SetMemoryLimit(rt,24*1024*1024); JS_SetMaxStackSize(rt,256*1024);
    ctx=JS_NewContext(rt); if(!ctx)return -1;
    control=JS_Eval(ctx,source,length,"script.js",JS_EVAL_TYPE_GLOBAL);
    if(JS_IsException(control))return save(control);
    return 0;
}
int poll(void) {
    for(int i=0;i<256 && JS_IsJobPending(rt);i++) {
        JSContext *jobctx;
        if(JS_ExecutePendingJob(rt,&jobctx)<0) return save(JS_EXCEPTION);
    }
    return save(invoke("poll",0,NULL));
}
int deliver(const char *json,int length) {
    JSValue arg=JS_ParseJSON(ctx,json,length,"broker.json");
    JSValue v=invoke("deliver",1,&arg); JS_FreeValue(ctx,arg);
    if(JS_IsException(v))return save(v); JS_FreeValue(ctx,v); return 0;
}
const char *result(void) {return output;}
