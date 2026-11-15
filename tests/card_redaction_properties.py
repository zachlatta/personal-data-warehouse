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


def unambiguous_insertion(context,pan):
    """Independent exhaustive checksum oracle for known insertion ground truth.

    Prefix numbers can accidentally form a SECOND valid PAN with inserted
    digits. Such inherently ambiguous cases have explicit conservative fixtures;
    exact-preservation properties construct one valid reading, without consulting
    the redactor or filtering the safe prose through it.
    """
    value=context+pan;atoms=list(re.finditer(r'[0-9]+',value))
    readings=[]
    for i,atom in enumerate(atoms):
        digits=''
        for j in range(i,len(atoms)):
            if j>i and not re.fullmatch(r'[ -]{1,3}',value[atoms[j-1].end():atoms[j].start()]):
                break
            digits+=atoms[j][0]
            if len(digits)>19:
                break
            if 13<=len(digits) and checksum_valid(digits):
                readings.append((atom.start(),atoms[j].end()))
    return readings==[(len(context),len(value))]


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
        prefix = '4' + digits(n-2)
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
        tokens = [phone, '+1-'+phone, phone2, date+'T'+time+'Z', date.replace('-','/'), time,
                  uuid, order, '9'*(40+rand(40)), digits(3)+'.'+digits(2)]
        # Fisher-Yates uses the same unsigned LCG in both language tests.
        for j in range(len(tokens)-1,0,-1):
            k=rand(j+1);tokens[j],tokens[k]=tokens[k],tokens[j]
        joiner = [' ','; ','\n',' | ','\t'][rand(5)]
        safe = joiner.join(tokens)
        yield f'preserve-{i}', safe, safe
        grouping, variant = formats[i%len(formats)]
        context = config['preceding_contexts'][(i//len(formats))%len(config['preceding_contexts'])]
        following = config['following_contexts'][(i//(len(formats)*len(config['preceding_contexts'])))%len(config['following_contexts'])]
        for attempt in range(1000):
            number=valid_number(sum(grouping))
            pan=render_groups(number,grouping,variant)
            if unambiguous_insertion(context,pan):
                break
        else:
            raise AssertionError('could not construct unambiguous synthetic PAN')
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
