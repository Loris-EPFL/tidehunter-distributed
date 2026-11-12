#!/usr/bin/env python3
"""Prepare an explicitly synthetic scratch brief; never patch the source brief."""
import argparse
from pathlib import Path
import shutil
import subprocess
import sys


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--scratch',type=Path,default=Path('/tmp/rwc10-build'))
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--provenance',type=Path,required=True)
    args=p.parse_args(); here=Path(__file__).resolve().parent; source=here.parent; scratch=args.scratch.resolve()
    if scratch==source or source in scratch.parents: raise ValueError('scratch must be outside source brief')
    marker=scratch/'rwc10-SYNTHETIC-owned'
    if scratch.exists():
        if not marker.is_file(): raise ValueError('refusing to replace unowned scratch directory: '+str(scratch))
        shutil.rmtree(scratch)
    ignore=shutil.ignore_patterns('__pycache__','rendered','rendered-real','rendered-corrections','SYNTHETIC-export','SYNTHETIC-edge-export',
                                  'SYNTHETIC-output','SYNTHETIC-data','SYNTHETIC-generated','SYNTHETIC-figures','EXCLUDED-*')
    subprocess.run([sys.executable,'-B',str(here/'prepare_patches.py')],check=True)
    shutil.copytree(source,scratch,ignore=ignore); marker.write_text('Owned synthetic scratch build; never campaign results.\n')
    for patch in ('main-tex-run10.patch','run9-security-run10.patch','verify-brief-run10.patch','synthetic-only.patch'):
        subprocess.run(['patch','--batch','-p1','-i',str(here/patch)],cwd=scratch,check=True)
    subprocess.run([sys.executable,'-B',str(scratch/'rwc10/build_rwc10_inputs.py'),'--brief',str(scratch),
                    '--output',str(args.output.resolve()),'--provenance',str(args.provenance.resolve()),'--register'],check=True)
    subprocess.run([sys.executable,'-B',str(scratch/'rwc10/plot_rwc10.py'),'--brief',str(scratch),'--output',str(args.output.resolve()),'--synthetic'],check=True)
    print('Prepared SYNTHETIC scratch only: '+str(scratch))
    print('Next: latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex (cwd '+str(scratch)+')')


if __name__=='__main__': main()
