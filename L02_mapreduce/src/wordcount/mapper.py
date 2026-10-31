#!/usr/bin/env python3
import re
import sys

WORD = re.compile(r"[a-z][a-z']*")

for line in sys.stdin:
    words = WORD.findall(line.lower())
    for word in words:
        print(f"{word}\t1")
