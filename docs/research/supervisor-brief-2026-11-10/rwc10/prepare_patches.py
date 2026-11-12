#!/usr/bin/env python3
"""Write exact unified diffs only. Never modify the sealed brief or tracked scripts."""
import difflib
from pathlib import Path


def replace_once(text,old,new):
    if text.count(old)!=1: raise ValueError('patch anchor changed: '+old[:100])
    return text.replace(old,new,1)


def patch(path,before,after):
    return ''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile='a/'+path,tofile='b/'+path))


def main():
    here=Path(__file__).resolve().parent; brief=here.parent
    original=(brief/'main.tex').read_text(); new=original
    prefix=''
    for flag,folder in [('ifdefined\\RWCtenSynthetic','rwc10/SYNTHETIC-generated'),('else','rwc10/generated')]:
        prefix+='\\'+flag+'\n'+''.join('\\input{'+folder+'/'+f+'}\n' for f in ('rwc10-numbers.tex','rwc10-findings.tex','rwc10-hooks.tex'))
    prefix+='\\fi\n'
    new=replace_once(new,'\\input{rwc/generated/rwc-figure-hooks.tex}\n','\\input{rwc/generated/rwc-figure-hooks.tex}\n'+prefix)
    new=replace_once(new,'\\item \\textbf{Run 9, remote reads and writes.} \\RWCFINDING{results}\n',
        '\\item \\textbf{Run 9, remote reads and writes.} \\RWCFINDING{results}\n'+
        '\\item \\textbf{Run 10, warm transport controls.} \\RWCtenFinding{scopewarm}\n'+
        '\\item \\textbf{Run 10, cold fabric controls.} \\RWCtenFinding{scopecold}\n')
    new=replace_once(new,'Run 9 adds a compact two-VM read/write campaign; earlier fleets remain separate.',
        'Runs 9 and 10 add separate compact two-VM read/write and fabric A/B campaigns; earlier fleets remain separate.')
    new=replace_once(new,
        r'Run 9, compact & \texttt{bench-41ecdf3f69fb47ec}; 10 November. One coordinator, one shard-WAL owner. & r0/r1; read/network floors also r2\\\bottomrule'+'\n',
        r'Run 9, compact & \texttt{bench-41ecdf3f69fb47ec}; 10 November. One coordinator, one shard-WAL owner. & r0/r1; read/network floors also r2\\'+'\n'+
        r'Run 10, compact & \texttt{bench-afdfe33817034a7c}; 10 November. One coordinator, one shard-WAL owner; warm security and polling controls, cold owner and NVMe-oF reads. & r0/r1\\\bottomrule'+'\n')
    new=replace_once(new,'\\begin{enumerate}\n','\\begingroup\\small\n\\begin{enumerate}[itemsep=1pt]\n')
    new=replace_once(new,'\\end{enumerate}\n\n\\clearpage\n\\section{Method and contracts}',
        '\\end{enumerate}\n\\endgroup\n\n\\clearpage\n\\section{Method and contracts}')
    new=replace_once(new,'A busy-poll A/B run must test the wake-up hypothesis.',
        'Run 10 ran the user-space Busy A/B; no isolated wake-up timer or kernel \\texttt{SO\\_BUSY\\_POLL} test.')
    new=replace_once(new,'\\textbf{Security and routes.}','\\textbf{Security and routes (runs 7--9).}')
    anchor='\\textbf{Not measured in runs 7 and 8.}'
    new=replace_once(new,anchor,'\\textbf{Run-10 arms.} Warm owner reads separate TCP mutual TLS Adaptive, TCP trusted Adaptive/Busy and verbs trusted Adaptive/Busy. Cold owner RPC is compared with direct kernel NVMe/RDMA, SPDK NVMe/RDMA and kernel NVMe/TCP. Busy spins in user space on both owner and coordinator; it is not kernel busy poll. Full contracts and run-9 security correction are in Section~\\ref{sec:rwc10}.\n\n'+anchor)
    new=replace_once(new,'\\input{rwc/section-rwc.tex}\n','\\input{rwc/section-rwc.tex}\n\\input{rwc10/section-rwc10.tex}\n')
    new=replace_once(new,'A/B remains unmeasured.','Run-10 matched ratios/CPU guide policy; no isolated wake-up timer.')
    new=replace_once(new,'Busy-poll versus adaptive A/B, same fleet/contract, CPU and tails. Failed event-write floors are unavailable. c5 reads are lane-limited, not transport limits.',
        'Run-10 trusted TCP/verbs Adaptive/Busy controls; inspect CPU, tails and CQ evidence. Busy also changes client progress, not kernel busy poll. Failed event-write floors stay unavailable.')
    new=replace_once(new,'No storage-path rewrite yet.','Run-10 cold controls do not supply linked residual timers.')
    new=replace_once(new,'\\section{Provenance and limits}\n','\\section{Provenance and limits}\\label{sec:provenance}\n\\RWCtenProvenance\n\n')
    new=replace_once(new,'\\textbf{Limits.} Runs 7 and 8','\\clearpage\n\\textbf{Limits.} Runs 7 and 8')
    (here/'main-tex-run10.patch').write_text(patch('main.tex',original,new))
    before=(brief/'rwc/section-rwc.tex').read_text()
    after=replace_once(before,'Network/security, index/history, routing and host SSD remain different.',
        'Network/security, index/history, routing and host SSD remain different. Run-9 TCP used mutual TLS and verbs trusted/no TLS, so their ratios include the security difference; run 10 adds trusted TCP rather than treating run 9 as like-for-like transport evidence.')
    (here/'run9-security-run10.patch').write_text(patch('rwc/section-rwc.tex',before,after))
    before=(brief/'verify_brief.py').read_text(); after=before
    after=replace_once(after,'assert len(pages) <= 48, len(pages)','assert len(pages) <= 56, len(pages)')
    anchor="    assert len(evidence['figures']) == 14\n"
    after=replace_once(after,anchor,"    rwc10_evidence = json.loads((HERE / manifest['rwc10']['manifest']).parent.joinpath('figure-evidence.json').read_text()) if 'rwc10' in manifest else {'figures': {}}\n"+anchor)
    after=replace_once(after,"len(figure_font_sizes) == len(evidence['figures']) + len(rwc_evidence['figures'])", "len(figure_font_sizes) == len(evidence['figures']) + len(rwc_evidence['figures']) + len(rwc10_evidence['figures'])")
    after=replace_once(after,"'figures': {'count': len(evidence['figures']) + len(rwc_evidence['figures']),", "'figures': {'count': len(evidence['figures']) + len(rwc_evidence['figures']) + len(rwc10_evidence['figures']),")
    after=replace_once(after,"'files': list(evidence['figures']) + list(rwc_evidence['figures']),", "'files': list(evidence['figures']) + list(rwc_evidence['figures']) + list(rwc10_evidence['figures']),")
    after=replace_once(after,'No saturation, measured CPU-cost, physical NAND or failover result; Sui capture adds sizes only, not replay performance',
        'No saturation, per-call CPU attribution, physical NAND or failover result; run 10 records process/cgroup CPU evidence; Sui capture adds sizes only, not replay performance')
    after=replace_once(after,'No infrastructure launched by document generation; authorized run-9 results imported',
        'No infrastructure launched by document generation; selected second-attempt run-9 and run-10 results imported; aborted attempts excluded')
    (here/'verify-brief-run10.patch').write_text(patch('verify_brief.py',before,after))
    print('Wrote main-tex-run10.patch, run9-security-run10.patch and verify-brief-run10.patch. None applied.')


if __name__=='__main__': main()
