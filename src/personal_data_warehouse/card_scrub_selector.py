"""Span selection for maintenance only. Seed values live in memory, never plans.

Keep one resolved selector alive through apply and post-resume verification.
Resolving a scrubbed seed is an error, not evidence that its copies are gone.
"""
import json
import re
import hashlib
import secrets
from dataclasses import dataclass, field

from personal_data_warehouse import card_redaction as cr
from personal_data_warehouse import credential_redaction

FIELD = r'(?:card(?:[ _-]*number)?|pan|cc|number|num)'
FIELD_KEY = re.compile('(?i)^'+FIELD+'$')
FIELD_BEFORE = re.compile(r'(?i)(?:^|[^A-Za-z0-9_])'+FIELD+cr._FIELD_SEPARATOR+'$')
CODE_AFTER = re.compile(r'(?i)^(?:[\s/|;#=&+,:\-\x22\x27]{1,24}|-[A-Za-z]{1,16}[ \t]+)'+cr._LABEL+cr._FIELD_SEPARATOR+r'[0-9]')
GAP = re.compile(r'[\s/|;#=&+,:\-\x22\x27]{0,24}')
EXPIRY_GAP = re.compile(r'(?i)[\s,;|/:-]{1,8}(?:(?:exp(?:iry|iration|ires)?|valid[ _-]*(?:thru|through))[\s:=]{0,8})?[0-9]{1,2}[ /-][0-9]{2,4}[\s,;|/:-]{0,8}')


@dataclass(repr=False)
class ScrubSelector:
    scope: str = 'all'
    seed_rows: tuple = ()
    seeds: tuple = field(default=(), repr=False)
    seed_revision: dict = field(default_factory=dict, repr=False)

    def __post_init__(self):
        if self.scope not in ('all', 'labelled', 'seed', 'credentials'):
            raise ValueError('unknown scrub scope')
        if self.scope == 'credentials' and (self.seed_rows or self.seeds or self.seed_revision):
            raise ValueError('credential scope does not accept card seeds')

    def seed_only(self, expected):
        """Bind a narrower policy to an already resolved checkpoint revision.

        The owner retains this object across connection replacement; no source
        read is needed after sanitation. A digest alone cannot recover a seed.
        """
        if self.specification != expected or not self.seeds or not self.seed_revision:
            raise ValueError('retained selector differs from checkpoint revision')
        return ScrubSelector('seed', self.seed_rows, self.seeds, dict(self.seed_revision))

    @property
    def specification(self):
        return {'scope': self.scope, 'seed_rows': list(self.seed_rows), 'seed_revision': self.seed_revision}

    def _edits(self, value, labelled=False, collect=None, code_side=None, selected_edges=None):
        tokens = cr._json_tokens(value)
        if tokens is not None:
            # Only immediate scalar fields can establish adjacency. Nested
            # objects never lend their code label to an unrelated sibling.
            fields = {}
            for i in range(2, len(tokens)):
                if tokens[i-1][0] == ':' and tokens[i-2][0].startswith('"'):
                    fields[i] = json.loads(tokens[i-2][0])
            edits = []; selected = set()
            for i, token in enumerate(tokens):
                raw = token[0]
                if raw[0] not in '"-0123456789': continue
                key = fields.get(i, '')
                neighbours = [j for j in (i-4, i+4) if j in fields and cr._CODE_KEY.fullmatch(fields[j]) and cr._code_token(tokens[j][0])
                              and tokens[min(i,j)+1][0] == ',']
                context = labelled if len(tokens)==1 else bool(FIELD_KEY.fullmatch(key))
                side = ('both' if len(neighbours)==2 else 'first' if neighbours and neighbours[0]<i else 'last' if neighbours else None)
                if len(tokens)==1: side=code_side
                inner_value = json.loads(raw) if raw.startswith('"') else raw
                # A decoded container consumes its wrapper's field context.
                # String wrappers may unwrap to another scalar, never to fields.
                edges = set()
                inner = self._edits(inner_value, context, collect, side, edges) if raw.startswith('"') else self._text(inner_value, context, collect, side, edges)
                if not inner: continue
                # Each code belongs to its nearest scalar reading. An edit to
                # an interior/opposite reading does not claim that code. Only
                # scalar string wrappers propagate edges; containers consume
                # their wrapper's adjacency context.
                selected.update(j for j in neighbours if ('first' if j<i else 'last') in edges)
                if len(tokens)==1 and selected_edges is not None:
                    selected_edges.update(edges)
                if raw.startswith('"'):
                    bounds = cr._json_string_bounds(raw)
                    edits.extend((token.start()+bounds[a],token.start()+bounds[b],json.dumps(r)[1:-1]) for a,b,r in inner)
                else:
                    edits.append((token.start(),token.end(),json.dumps(cr._apply_edits(raw,inner))))
            for i in selected:
                token=tokens[i]
                edits.append((token.start(),token.end(),'"[redacted]"'))
            return sorted(edits)
        return self._text(value, labelled, collect, code_side, selected_edges)

    def _text(self, value, labelled, collect, code_side=None, selected_edges=None):
        chars=[]; bounds=[0]; pos=0
        while pos<len(value):
            match=cr._ESCAPE.match(value,pos) if value[pos]=='\\' else None
            chars.append(chr(int(match[1],16)) if match else value[pos])
            pos=match.end() if match else pos+1
            bounds.append(pos)
        decoded=''.join(chars); edits=[]
        codes=list(cr._CODE.finditer(decoded))
        readings=list(cr.card_spans(decoded, merge=False))
        for a,b in readings:
            digits=decoded[a:b].replace(' ','').replace('-','')
            if collect is not None: collect.add(digits)
            nearby=[m for m in codes if (b<=m.start()<=b+80 and (GAP.fullmatch(decoded[b:m.start()]) or EXPIRY_GAP.fullmatch(decoded[b:m.start()]))) or
                    (m.end()<=a and GAP.fullmatch(decoded[m.end():a]))]
            adjacent = (code_side in ('first','both') and a==readings[0][0] or
                        code_side in ('last','both') and b==readings[-1][1])
            if self.scope == 'seed':
                if digits not in self.seeds: continue
            elif not (adjacent or labelled and not decoded[:a].strip() or digits in self.seeds or FIELD_BEFORE.search(decoded[max(0,a-128):a]) or CODE_AFTER.search(decoded[b:b+128]) or nearby): continue
            if selected_edges is not None:
                if a==readings[0][0]: selected_edges.add('first')
                if b==readings[-1][1]: selected_edges.add('last')
            edits.append((a,b,'[card ****'+digits[-4:]+']'))
            end=b
            while (code:=cr._ADJACENT_CODE.match(decoded,end)) and cr.standalone_end(decoded,code.end()):
                if any(x<code.end() and y>code.start(1) for x,y in readings): break
                edits.append((code.start(1),code.end(),'[redacted]')); end=code.end()
            edits.extend((*m.span(2),'[redacted]') for m in nearby)
            # Decorative suffixes are supported by the existing detector.
            if CODE_AFTER.search(decoded[b:b+128]):
                match=next((m for m in codes if b<=m.start()<b+128),None)
                if match: edits.append((*match.span(2),'[redacted]'))
        merged=[]
        for a,b,r in sorted(edits):
            if merged and a<merged[-1][1]:
                x,y,s=merged[-1]; merged[-1]=(x,max(y,b),s if s.startswith('[card ') else r)
            else: merged.append((a,b,r))
        return [(bounds[a],bounds[b],r) for a,b,r in merged]

    def count(self, value):
        if self.scope=='all': return cr.sensitive_count(value)
        if self.scope=='credentials': return credential_redaction.credential_count(value)
        return sum('[card ' in r for _,_,r in self._edits(value))

    def count_row(self, row):
        if self.scope == 'all':
            return sum(cr.sensitive_count(v if isinstance(v,str) else json.dumps(v)) for v in row.values() if isinstance(v,(str,dict,list)))
        return sum(self.count(v if isinstance(v,str) else json.dumps(v)) for v in row.values() if isinstance(v,(str,dict,list)))

    def redact(self, value):
        if self.scope=='credentials': return credential_redaction.redact_credentials(value)
        return cr.redact(value) if self.scope=='all' else cr._apply_edits(value,self._edits(value))

    def redact_row(self, row):
        if self.scope=='all': return cr.redact_row(row)
        return {k:self.redact(v) if isinstance(v,str) else json.loads(self.redact(json.dumps(v))) if isinstance(v,(dict,list)) else v for k,v in row.items()}


def resolve_selector(warehouse, scope='all', seed_rows=(), *, expected=None):
    from personal_data_warehouse.postgres import POSTGRES_TABLES, _AI_CONVERSATION_EVENT_TABLES
    selector=ScrubSelector(scope,tuple(seed_rows))
    seeds=set()
    salt = expected['seed_revision']['salt'] if expected and seed_rows else secrets.token_hex(32)
    revisions=[]
    for identity in seed_rows:
        table,sep,pk=identity.partition(':')
        if not sep or table not in _AI_CONVERSATION_EVENT_TABLES: raise ValueError('invalid seed table')
        try: key=json.loads(pk)
        except ValueError: raise ValueError('seed key must be a JSON primary-key object') from None
        keys=POSTGRES_TABLES[table].primary_key
        if not isinstance(key,dict) or set(key)!=set(keys): raise ValueError('invalid seed primary key')
        rows=warehouse._query_dicts(f'SELECT * FROM @{table} WHERE '+' AND '.join(f'{k}=%s' for k in keys),tuple(key[k] for k in keys))
        revisions.append(rows)
        found=set()
        for row in rows:
            for v in row.values():
                if isinstance(v,(str,dict,list)): selector._edits(v if isinstance(v,str) else json.dumps(v),collect=found)
        if not found: raise ValueError('seed unavailable or already sanitized; retain the original selector in memory')
        seeds.update(found)
    selector.seeds=tuple(sorted(seeds))
    if seed_rows:
        digest=hashlib.sha256((salt+json.dumps(revisions,sort_keys=True,default=str)).encode()).hexdigest()
        selector.seed_revision={'salt':salt,'digest':digest}
    if expected is not None and selector.specification != expected:
        raise ValueError('seed revision changed; rediscovery required before any mutation')
    return selector


def add_selector_arguments(parser):
    parser.add_argument('--scope',choices=('all','labelled','seed','credentials'),default='all')
    parser.add_argument('--seed-row',action='append',default=[],help='logical_table:JSON primary-key object; repeatable, IDs only')


def selector_from_args(args, expected=None):
    if not args.seed_row:
        selector = ScrubSelector(args.scope)
        if expected is not None and selector.specification != expected:
            raise ValueError('plan selector differs from current selector')
        return selector
    import os
    from personal_data_warehouse.postgres import PostgresWarehouse
    warehouse=PostgresWarehouse(os.environ['POSTGRES_DATABASE_URL'],initialize=False)
    try:
        warehouse._connection.set_session(readonly=True,autocommit=True)
        return resolve_selector(warehouse,args.scope,args.seed_row,expected=expected)
    finally: warehouse.close()


def validate_specification(spec):
    from personal_data_warehouse.postgres import POSTGRES_TABLES, _AI_CONVERSATION_EVENT_TABLES
    if not isinstance(spec,dict) or set(spec)!={'scope','seed_rows','seed_revision'} or spec['scope'] not in ('all','labelled','seed','credentials') or not isinstance(spec['seed_rows'],list):
        raise ValueError('invalid selector specification')
    if spec['scope']=='credentials' and (spec['seed_rows'] or spec['seed_revision']):
        raise ValueError('credential scope does not accept card seeds')
    revision=spec['seed_revision']
    if spec['seed_rows'] and (not isinstance(revision,dict) or set(revision)!={'salt','digest'} or any(not isinstance(v,str) or not re.fullmatch('[a-f0-9]{64}',v) for v in revision.values())):
        raise ValueError('invalid seed revision')
    for identity in spec['seed_rows']:
        if not isinstance(identity,str): raise ValueError('invalid seed identity')
        table,_,raw=identity.partition(':')
        if table not in _AI_CONVERSATION_EVENT_TABLES: raise ValueError('invalid seed table')
        try: key=json.loads(raw)
        except ValueError: raise ValueError('invalid seed identity') from None
        if not isinstance(key,dict) or set(key)!=set(POSTGRES_TABLES[table].primary_key): raise ValueError('invalid seed key')
        if any(not isinstance(v,(str,int)) or isinstance(v,bool) or isinstance(v,str) and cr.card_count(v) for v in key.values()):
            raise ValueError('sensitive or invalid seed identity')
