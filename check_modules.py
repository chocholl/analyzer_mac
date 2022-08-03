import os
import sys
import importlib

needed = {}

with open('modules.txt', 'r') as f:
        for l in f.readlines():
                if(l != ''):
                        l = l.replace('\n', '')
                        module = l.split(' ')[0]
                        package = l.split(' ')[1]
                        needed[module]= package

for m in needed:
        try:
                print('Checking if ' + m + ' installed...')
                importlib.import_module(m)
        except ImportError as ex:
                print(m + ' not found... Installing...')
                os.system("pip install " + needed[m])

print('')
print('Done.')
