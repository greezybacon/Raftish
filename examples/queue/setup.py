import os, os.path
import sys

parent_of = os.path.dirname
base = parent_of(parent_of(parent_of(os.path.abspath(__file__))))
src = os.path.join(base, 'src')

if src not in sys.path:
    sys.path.append(src)