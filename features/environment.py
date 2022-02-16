import os, os.path
import sys

base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_base = os.path.join(base, 'src')
if src_base not in sys.path:
    sys.path.append(src_base)

def after_scenario(context, scenario):
    if hasattr(context, 'clusters'):
        for C in context.clusters:
            C.shutdown()