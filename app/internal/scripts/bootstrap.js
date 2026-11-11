// The host holds this returned controller outside the guest global object.
// Capture intrinsics before user code can replace globals or prototypes.
((code, args) => {
  const stringify = JSON.stringify.bind(JSON);
  const parse = JSON.parse.bind(JSON);
  const splice = Function.call.bind(Array.prototype.splice);
  const push = Function.call.bind(Array.prototype.push);
  const keys = Object.keys;
  const freeze = Object.freeze;
  const NativePromise = Promise;
  const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
  const error = (e) => ({code: typeof e?.code === 'string' ? e.code : 'script_error',
    message: String(e?.message ?? e).slice(0,1000), hint: String(e?.hint ?? '').slice(0,1000)});
  const encode = v => stringify(v, (_, x) => typeof x === 'bigint' ? String(x) : x);
  const immutable = x => {if(x && typeof x === 'object') {for(const k of keys(x)) immutable(x[k]); freeze(x);}return x;};
  let calls=[], waiting=new Map(), next=0, done=false, value=null, failure=null;
  const request=(method,input)=>new NativePromise((resolve,reject)=>{
    if(++next>12) {reject({code:'operation_limit',message:'At most 12 operations per run'});return;}
    // Copy through JSON now: later script mutation cannot alter a queued call.
    const copied=parse(encode(input ?? {}));
    waiting.set(next,{resolve,reject});push(calls,{id:next,method,input:copied});
  });
  const pdw=freeze(Object.fromEntries(['search','context','columns','schema','sql'].map(
    method=>[method,freeze(input=>request(method,input))])));
  try {
    new AsyncFunction('pdw','args','"use strict";\n'+code)(pdw,immutable(args))
      .then(v=>{value=v??null;done=true;},e=>{failure=error(e);done=true;});
  } catch(e) {failure=error(e);done=true;}
  return {
    poll:()=>encode({calls:splice(calls,0),done,value:done?value:null,error:failure}),
    deliver: responses=>{for(const r of responses){const p=waiting.get(r.id);if(!p)throw Error('unknown call');waiting.delete(r.id);r.error?p.reject(r.error):p.resolve(r.value);}}
  };
})(__CODE__, __ARGS__)
