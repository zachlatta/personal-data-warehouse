"""Independent fixed-seed corpus generator, mirrored in Go, configured by JSON."""
import json
from pathlib import Path


def checksum_valid(value):
    return sum((int(c)*2-9 if int(c)>4 else int(c)*2) if i%2 else int(c)
               for i,c in enumerate(reversed(value))) % 10 == 0


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
    formats = [(g,s) for g in config['groupings'] for s in ([''] if len(g)==1 else [' ','-'])]
    for i in range(config['iterations']):
        area = str(200+rand(700)); exchange=str(200+rand(700)); line=digits(4)
        phone = area+'-'+exchange+'-'+line
        phone2 = '(202) '+str(200+rand(700))+'-'+digits(4)
        date = f'{2000+rand(40):04}-{1+rand(12):02}-{1+rand(28):02}'
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
        grouping, sep = formats[i%len(formats)]
        delimiter = config['delimiters'][(i//len(formats))%len(config['delimiters'])]
        number = valid_number(sum(grouping))
        parts=[];pos=0
        for n in grouping:
            parts.append(number[pos:pos+n]);pos+=n
        pan=sep.join(parts)
        replacement='[card ****'+number[-4:]+']'
        # Explicit ambiguity policy: 4/4/4/4 + isolated 3 can be PAN+CVV.
        # Both are suppressed, retaining digits only from the confirmed PAN.
        bare_end = not (delimiter[0].isalnum() or delimiter[0] in '_-/:' or
                        (delimiter[0]=='.' and len(delimiter)>1 and delimiter[1].isdigit()))
        if sep==' ' and grouping==[4,4,4,4,3] and checksum_valid(number[:16]) and bare_end:
            replacement='[card ****'+number[12:16]+'] [redacted]'
        at=rand(len(tokens)+1)
        prefix=joiner.join(tokens[:at])+' | '
        suffix=delimiter+' | '+joiner.join(tokens[at:])
        yield f'insert-{i}', prefix+pan+suffix, prefix+replacement+suffix
