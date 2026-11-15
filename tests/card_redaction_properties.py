"""Independent fixed-seed corpus generator, mirrored in Go, configured by JSON."""
import json
import re
from pathlib import Path


def checksum_valid(value):
    return sum((int(c)*2-9 if int(c)>4 else int(c)*2) if i%2 else int(c)
               for i,c in enumerate(reversed(value))) % 10 == 0


def render_groups(number,grouping,variant):
    separators = {'plain':[''], 'space':[' '], 'dash':['-'], 'double':['  '], 'triple':['   '],
                  'mixed':['-',' '], 'mixed_runs':[' -','- ',' - ']}
    result='';pos=0
    for i,n in enumerate(grouping):
        if i:
            options=separators[variant]
            result+=options[(i-1)%len(options)]
        result+=number[pos:pos+n];pos+=n
    return result


def unambiguous_insertion(context, pan, following, layouts):
    """Independent exhaustive checksum oracle for known insertion ground truth.

    Prefix numbers can accidentally form a SECOND valid PAN with inserted
    digits. Such inherently ambiguous cases have explicit conservative fixtures;
    exact-preservation properties construct one valid reading, without consulting
    the redactor or filtering the safe prose through it.
    """
    value=context+pan+following;atoms=list(re.finditer(r'[0-9]+',value))
    readings=[]
    for i,atom in enumerate(atoms):
        digits=''
        groups=[]
        for j in range(i,len(atoms)):
            if j>i and not re.fullmatch(r'[ -]{1,3}',value[atoms[j-1].end():atoms[j].start()]):
                break
            digits+=atoms[j][0]
            groups.append(len(atoms[j][0]))
            if len(digits)>19:
                break
            if groups in layouts and checksum_valid(digits):
                readings.append((atom.start(),atoms[j].end()))
    return readings==[(len(context),len(context)+len(pan))]


def encoded_document(pan, replacement, depth, mixed, encoded_slash):
    encoded = ''.join('\\u%04x'%ord(c) if c.isdigit() and (not mixed or i%2==0) else c
                      for i,c in enumerate(pan))
    value = '{ "text":"'+encoded+' CVV2: 987", "keep":1e400, "dup":1,"dup":2, "u":"\\u00e9" }'
    expected = value.replace(encoded,replacement).replace(' CVV2: 987',' CVV2: [redacted]') if replacement else value
    if encoded_slash:
        value=value.replace('\\',r'\u005c');expected=expected.replace('\\',r'\u005c')
    for _ in range(depth-1):
        value=json.dumps({'payload':value},separators=(',',':'))
        expected=json.dumps({'payload':expected},separators=(',',':'))
        if encoded_slash:
            value=value.replace('\\\\',r'\u005c');expected=expected.replace('\\\\',r'\u005c')
    return value,expected


def property_cases():
    config = json.loads((Path(__file__).parent/'fixtures/card_redaction_cases.json').read_text())['property']
    state = config['seed']
    def rand(n):
        nonlocal state
        state = (1664525*state + 1013904223) & 0xffffffff
        return state % n
    def digits(n):
        return ''.join(str(rand(10)) for _ in range(n))
    def valid_number(n):
        prefix = ('4' if n in (13,16,19) else '6') + digits(n-2)
        return next(prefix+str(i) for i in range(10) if checksum_valid(prefix+str(i)))
    formats = [(g,s) for g in config['groupings'] for s in (['plain'] if len(g)==1 else config['separator_variants'])]
    for i in range(config['iterations']):
        area = str(200+rand(700)); exchange=str(200+rand(700)); line=digits(4)
        phone = area+'-'+exchange+'-'+line
        phone2 = '(202) '+str(200+rand(700))+'-'+digits(4)
        date = f'{1000+rand(2000):04}-{1+rand(12):02}-{1+rand(28):02}'
        time = f'{rand(24):02}:{rand(60):02}:{rand(60):02}'
        uuid = '-'.join(''.join('0123456789abcdef'[rand(16)] for _ in range(n)) for n in (8,4,4,4,12))
        valid_order = valid_number(16)
        order = valid_order[:-1] + str((int(valid_order[-1])+1)%10)
        # Layout-free joining would corrupt these independent identifiers.
        # Exercise both valid and invalid aggregate checksums, never filtering
        # preservation cases through the production redactor.
        quantity=next(str(n) for n in range(10,100) if checksum_valid(order+str(n)))
        aggregate=valid_number(18)
        aggregate_bad=aggregate[:-1]+str((int(aggregate[-1])+1)%10)
        non_layout_groups = [6,6,6] if i%3==0 else [4,4,4,5+i%3]
        non_layout=render_groups(valid_number(sum(non_layout_groups)), non_layout_groups, config['separator_variants'][i%6])
        tokens = [phone, '+1-'+phone, phone2, date+'T'+time+'Z', date.replace('-','/'), time,
                  uuid, order, '9'*(40+rand(40)), digits(3)+'.'+digits(2),
                  order+' '+quantity, ' '.join((aggregate[:12],aggregate[12:16],aggregate[16:])),
                  ' '.join((aggregate_bad[:12],aggregate_bad[12:16],aggregate_bad[16:])), 'id='+non_layout+';']
        # Fisher-Yates uses the same unsigned LCG in both language tests.
        for j in range(len(tokens)-1,0,-1):
            k=rand(j+1);tokens[j],tokens[k]=tokens[k],tokens[j]
        joiner = [' ','; ','\n',' | ','\t'][rand(5)]
        safe = joiner.join(tokens)
        yield f'preserve-{i}', safe, safe
        grouping, variant = formats[i%len(formats)]
        context = config['preceding_contexts'][(i//len(formats))%len(config['preceding_contexts'])]
        # A checksum-valid four-digit line plus a 13-digit PAN is invariably
        # another valid 17-digit layout. Construct unambiguous phone context
        # independently; the unfiltered safe prose above keeps the original.
        context_line = line
        while checksum_valid(context_line) or checksum_valid(context_line+'0'):
            context_line = context_line[:-1]+str((int(context_line[-1])+1)%10)
        context_phone = area+'-'+exchange+'-'+context_line
        context = {'$four_digits ': context_line+' ', '$phone ': context_phone+' ', '$phone_prefix ': area+' '+exchange+' ',
                   '$date ': date+' ', '$order ': order+' '}.get(context, context)
        following = config['following_contexts'][(i//(len(formats)*len(config['preceding_contexts'])))%len(config['following_contexts'])]
        for attempt in range(1000):
            number=valid_number(sum(grouping))
            pan=render_groups(number,grouping,variant)
            if unambiguous_insertion(context,pan,following['input'],config['groupings']):
                break
        else:
            raise AssertionError(f'could not construct unambiguous synthetic PAN at {i}, layout {grouping}, variant {variant}')
        replacement='[card ****'+number[-4:]+']'
        at=rand(len(tokens)+1)
        prefix=joiner.join(tokens[:at])+' | '+context
        suffix=' | '+joiner.join(tokens[at:])
        yield f'insert-{i}', prefix+pan+following['input']+suffix, prefix+replacement+following['expected']+suffix
    tokens = config['permutation_tokens']
    for i,a in enumerate(tokens):
        for j,b in enumerate(tokens):
            for k,c in enumerate(tokens):
                if len({i,j,k})!=3:
                    continue
                for n,joiner in enumerate(config['permutation_joiners']):
                    value=joiner.join((a,b,c))
                    yield f'permutation-preserve-{i}-{j}-{k}-{n}', value, value
                    yield f'permutation-insert-{i}-{j}-{k}-{n}', '4111111111111111 | '+value, '[card ****1111] | '+value
    for i in range(config['encoding_iterations']):
        grouping,variant=formats[i%len(formats)]
        for attempt in range(1000):
            number=valid_number(sum(grouping));pan=render_groups(number,grouping,variant)
            if unambiguous_insertion('',pan,' CVV2: 987',config['groupings']):
                break
        else:
            raise AssertionError('could not construct unambiguous encoded PAN')
        options=(1+i%3, bool((i//6)%2), bool((i//3)%2))
        value,expected=encoded_document(pan,'[card ****'+number[-4:]+']',*options)
        yield f'encoded-insert-{i}',value,expected
        value,expected=encoded_document('4111111111111112',None,*options)
        yield f'encoded-preserve-{i}',value,expected
