#!/usr/bin/env python3
"""Decode archive protobuf fields and BCS payloads, then emit bounded CSV data."""
import collections
import csv
import json
import math
import pathlib
import subprocess
import sys
from download import STARTS, COUNT

ROOT = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else None
DECODER = (ROOT.parent if ROOT and ROOT.name == 'primary' else ROOT) / 'target/release/sui-workload' if ROOT else None

def varint(buf, pos):
    result = 0
    for shift in range(0, 70, 7):
        if pos >= len(buf): raise ValueError('truncated varint')
        byte = buf[pos]; pos += 1
        result |= (byte & 127) << shift
        if not byte & 128: return result, pos
    raise ValueError('varint overflow')

def fields(buf):
    pos = 0
    while pos < len(buf):
        tag, pos = varint(buf, pos)
        field, wire = tag >> 3, tag & 7
        if not field: raise ValueError('zero field')
        if wire == 0: value, pos = varint(buf, pos)
        elif wire == 2:
            size, pos = varint(buf, pos)
            if size > len(buf)-pos: raise ValueError('truncated field')
            value = buf[pos:pos+size]; pos += size
        elif wire in (1, 5):
            size = 8 if wire == 1 else 4
            if size > len(buf)-pos: raise ValueError('truncated fixed field')
            value = buf[pos:pos+size]; pos += size
        else: raise ValueError('unsupported wire type')
        yield field, value

def allf(buf, n): return [v for k,v in fields(buf) if k == n]
def one(buf, n):
    values = allf(buf,n)
    if len(values) != 1: raise ValueError(f'field {n}: got {len(values)}')
    return values[0]
def payload(buf): return one(one(buf, 1), 2)
def variant(x):
    if isinstance(x, str): return x, None
    if isinstance(x, dict) and len(x) == 1: return next(iter(x.items()))
    raise ValueError(f'invalid variant: {x!r}')

def decode_many(items):
    raw = ''.join(f'{kind}\t{data.hex()}\n' for kind,data in items).encode()
    proc = subprocess.run([DECODER], input=raw, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    result = [json.loads(line) for line in proc.stdout.splitlines()]
    if len(result) != len(items): raise ValueError('decoder output count mismatch')
    return result

def rows(path, header):
    file = path.open('w', newline=''); writer = csv.writer(file); writer.writerow(header); return file, writer

def object_type(obj):
    kind, data = variant(obj['data'])
    if kind != 'Struct': return 'package'
    typ = data['type_']
    return typ if isinstance(typ,str) else str(next(iter(typ.values())))

def reconstruct_inputs(effects, data, kind, program):
    inputs = {}
    shared = False
    for change in effects.get('changed_objects', []):
        state, value = variant(change['input_state'])
        if state == 'Exist':
            inputs[change['object_id']] = (value['version'], value['owner'])
    for change in effects.get('unchanged_consensus_objects', []):
        typ, value = variant(change['kind'])
        if typ == 'ReadOnlyRoot':
            inputs[change['object_id']] = (value['version'], {'shared': 0})
    if kind == 'ProgrammableTransaction':
        for item in program['inputs']:
            if 'Object' not in item:
                continue
            typ, value = variant(item['Object'])
            if typ == 'Shared':
                shared = True
            elif typ in ('ImmutableOrOwned', 'Receiving'):
                inputs.setdefault(value['object_id'], (value['version'], None))
        for gas in data['gas_payment']['objects']:
            inputs.setdefault(gas['object_id'], (gas['version'], None))
    return inputs, shared

def version_age(created, oid, version, checkpoint, transaction):
    origin = created.get((oid, version))
    if origin is None:
        return '', '', '', ''
    return origin[0], origin[1], checkpoint - origin[0], transaction - origin[1]

def read_checkpoint(path):
    raw = subprocess.check_output(['zstd','-q','-d','-c',str(path)])
    fs = list(fields(raw)); seq = next(v for k,v in fs if k == 1)
    tx = [v for k,v in fs if k == 6]
    objects = allf(one(raw,7),1)
    items = [('summary',payload(one(raw,3)))]
    for t in tx:
        items += [('transaction',payload(one(t,2))),('effects',payload(one(t,4)))]
        events=allf(t,5)
        if events: items.append(('events',payload(events[0])))
    for obj in objects: items.append(('object',payload(obj)))
    decoded = iter(decode_many(items)); summary=next(decoded); result=[]
    for t in tx:
        tb=payload(one(t,2)); eb=payload(one(t,4)); tr=next(decoded); eff=next(decoded)
        events=allf(t,5); evb=payload(events[0]) if events else b''; ev=next(decoded) if events else {'events':[], 'sizes':[]}
        if tr['digest'] != next(iter(eff.values()))['transaction_digest']: raise ValueError(f'transaction digest mismatch in {seq}')
        result.append((tr['value'],eff,ev,len(tb),len(eb),len(evb)))
    objmap={}
    for obj in objects:
        val=next(decoded); key=(one(obj,2).decode(),one(obj,3)); objmap[key]=(val,len(payload(obj)))
    return seq,summary,result,objmap

def main():
    ROOT.mkdir(exist_ok=True,parents=True)
    fs=[]
    def writer(name, header):
        f,w=rows(ROOT/name,header);fs.append(f);return w
    cp=writer('checkpoints.csv',['window','sequence','timestamp_ms','transactions','interval_ms','raw_bytes','events','objects_written','transaction_bcs_bytes','effects_bcs_bytes','events_bcs_bytes','output_object_bcs_bytes'])
    txw=writer('transactions.csv',['window','checkpoint','index','digest','kind','commands','shared','success','gas_computation','gas_storage','gas_rebate','input_objects','output_objects','created','mutated','deleted','wrapped','unwrapped','transaction_bytes','effects_bytes','events_bytes','class'])
    access=writer('input_objects.csv',['window','checkpoint','transaction_index','object_id','version','shared','object_type','version_created_checkpoint','version_created_transaction','age_checkpoints','age_transactions'])
    out=writer('output_objects.csv',['window','checkpoint','transaction_index','object_id','version','object_type','bcs_bytes','change'])
    cmd=writer('commands.csv',['window','checkpoint','transaction_index','command','target'])
    evw=writer('events.csv',['window','checkpoint','transaction_index','event_type','bcs_bytes'])
    validation=writer('digest_validation.csv',['window','transactions_checked','mismatches'])
    for wi,start in enumerate(STARTS):
        created={}; global_index=0; prior=None
        for seq in range(start,start+COUNT):
            path=ROOT/'raw'/f'{seq}.binpb.zst'
            n,s,transactions,objects=read_checkpoint(path)
            if n != seq: raise ValueError('sequence mismatch')
            events_count=written=txbytes=fxbytes=evbytes=objbytes=0
            for index,(tr, eff0, events, tlen, elen, evlen) in enumerate(transactions):
                for event,size in zip(events['events'],events['sizes'],strict=True):
                    evw.writerow([wi,seq,index,event['type'],size])
                data=next(iter(tr.values())); e=next(iter(eff0.values())); kind,program=variant(data['kind']); commands=[] if kind!='ProgrammableTransaction' else program['commands']; shared=False
                for command in commands:
                    name, value=variant(command); target=''
                    if name == 'MoveCall': target='::'.join((value['package'],value['module'],value['function']))
                    cmd.writerow([wi,seq,index,name,target])
                changes=e.get('changed_objects',[]); inputs,shared=reconstruct_inputs(e,data,kind,program); outputs=[]; counts=collections.Counter()
                for c in changes:
                    oid=c['object_id']; state,inp=variant(c['input_state']); output,ov=variant(c['output_state']); op=c['id_operation']
                    if output in ('ObjectWrite','PackageWrite'):
                        version=e['lamport_version'] if output=='ObjectWrite' else ov['version']
                        change='created' if op=='Created' else ('unwrapped' if state!='Exist' else 'mutated')
                        outputs.append((oid,version,change)); counts[change]+=1
                    elif output=='NotExist':
                        if op=='Deleted': counts['deleted']+=1
                        elif state=='Exist': counts['wrapped']+=1
                for oid,(ver,owner) in inputs.items():
                    obj=objects.get((oid,ver)); ot=object_type(obj[0]) if obj else ''
                    is_shared=isinstance(owner,dict) and 'shared' in owner
                    if is_shared: shared=True
                    age=version_age(created,oid,ver,seq,global_index)
                    access.writerow([wi,seq,index,oid,ver,int(is_shared),ot,*age])
                for oid,ver,change in outputs:
                    obj=objects.get((oid,ver))
                    if not obj: raise ValueError(f'missing output object {seq} {oid} {ver}')
                    objbytes+=obj[1]; out.writerow([wi,seq,index,oid,ver,object_type(obj[0]),obj[1],change]);created[(oid,ver)]=(seq,global_index)
                events_count+=len(events['events']); written+=len(outputs);txbytes+=tlen;fxbytes+=elen;evbytes+=evlen
                gas=e['gas_used']; status=e['status']; success=status=='Success'; names=[variant(c)[0] for c in commands]
                functions=[v['function'].lower() for name,v in map(variant,commands) if name=='MoveCall']
                classification=('system' if kind!='ProgrammableTransaction' else 'swap_name' if any('swap' in f for f in functions) else 'mint_name' if any('mint' in f for f in functions) else 'transfer' if 'TransferObjects' in names else 'other_programmable')
                txw.writerow([wi,seq,index,e['transaction_digest'],kind,len(commands),int(shared),int(success),gas['computation_cost'],gas['storage_cost'],gas['storage_rebate'],len(inputs),len(outputs),counts['created'],counts['mutated'],counts['deleted'],counts['wrapped'],counts['unwrapped'],tlen,elen,evlen,classification]);global_index+=1
            stamp=s['timestamp_ms'];cp.writerow([wi,seq,stamp,len(transactions),stamp-prior if prior is not None else '',path.stat().st_size,events_count,written,txbytes,fxbytes,evbytes,objbytes]);prior=stamp
        if global_index < 100: raise ValueError(f'window {wi} has fewer than 100 transactions')
        validation.writerow([wi,global_index,0])
        print(f'window {wi} complete',flush=True)
    for f in fs:f.close()
if __name__=='__main__':main()
