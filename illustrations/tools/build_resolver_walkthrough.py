"""Build the blog walkthrough using notebook 1's resolve_group function.

Run with pandas, Pillow and rsvg-convert available. Geometry supplies the small
fixture's candidate distances; assignment decisions execute the notebook function.
"""
from pathlib import Path
from collections import defaultdict
import ast, json, math, html, subprocess, tempfile, textwrap
import pandas as pd
from PIL import Image

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'illustrations' / 'identity'
source = ast.parse((ROOT / 'notebooks/01_identity_tracks_and_late_data.py').read_text())
fn = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == 'resolve_group')
cols = ['obs_id', 'MMSI', 'event_time_s', 'LON', 'LAT', 'track_id', 'track_status']
ns = dict(pd=pd, defaultdict=defaultdict, TIE_MARGIN_M=100., ASSIGN_COLS=cols)
exec(compile(ast.Module(body=[fn], type_ignores=[]), '<notebook resolve_group>', 'exec'), ns)
points = [('A0',0,1.),('A10',10,1.015),('A20',20,1.03),('A30',30,1.045),
          ('B0',0,1.3),('B10',10,1.285),('B20',20,1.27),('B30',30,1.255)]
P = {n: dict(obs_id=n,MMSI=235000001,event_time_s=t*60,LON=x,LAT=51.,kind='position') for n,t,x in points}
def distance(p,q):
    a,b=map(math.radians,[p['LAT'],q['LAT']]);dl=math.radians(q['LON']-p['LON'])
    return 6371008.8*2*math.asin(math.sqrt(math.sin((b-a)/2)**2+math.cos(a)*math.cos(b)*math.sin(dl/2)**2))
def resolve(names):
    rows=[P[n] for n in names]
    for q in names:
        for p in names:
            dt=P[q]['event_time_s']-P[p]['event_time_s']
            d=distance(P[p],P[q])
            if 0<dt<=720 and d/dt<=30*1852/3600:
                rows.append(dict(kind='edge',obs_id=q,prev_id=p,distance_m=d))
    return ns['resolve_group'](pd.DataFrame(rows))
def assignments(names): return resolve(names).set_index('obs_id').to_dict('index')
initial=['A0','B0','B10','A20','B20','A30','B30']
replay=['A0','B0','A10','B10','A20','B20','A30','B30']
before,after=assignments(initial),assignments(replay)
assert before['A0']['track_id']!=before['A20']['track_id']
assert after['A0']['track_id']==after['A10']['track_id']==after['A20']['track_id']==after['A30']['track_id']
assert after['B0']['track_id']==after['B30']['track_id']!=after['A0']['track_id']
assert before['A0']['track_status']=='tentative' and after['A0']['track_status']=='supported'

def esc(s):return html.escape(str(s))
def text(x,y,s,size=18,color='#172321',weight=400,anchor='start'):
    return f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" font-weight="{weight}" text-anchor="{anchor}">{esc(s)}</text>'
def rect(x,y,w,h,fill='#f5f8f7',stroke='none'):
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" stroke="{stroke}"/>'
def line(x,y,xx,yy,c='#dce5e2',width=3,dash=''):
    return f'<line x1="{x}" y1="{y}" x2="{xx}" y2="{yy}" stroke="{c}" stroke-width="{width}" stroke-dasharray="{dash}"/>'
def xy(n):return (150+P[n]['event_time_s']/60*29,240 if n[0]=='A' else 375)
def color(tid):return '#cb6329' if 'B' in tid else '#087f8c' if tid=='trk_A0' else '#4b66aa'
frames=[]
for phase,order in [('Initial reports',initial),('Replay with late A10',replay)]:
    for step,current in enumerate(order):
        names=order[:step+1];a=assignments(names);prev=assignments(names[:-1]) if step else {}
        endpoints={}
        for n in names[:-1]:
            tid=prev[n]['track_id']
            if tid:endpoints[tid]=n
        choices=[]
        for tid,p in endpoints.items():
            dt=P[current]['event_time_s']-P[p]['event_time_s'];d=distance(P[p],P[current])
            if 0<dt<=720 and d/dt<=30*1852/3600:choices.append((d,p,tid))
        choices.sort();tid=a[current]['track_id']
        if choices:
            p=choices[0][1];action=f'Extend {tid}: {p} → {current}'
            detail=f'{(P[current]["event_time_s"]-P[p]["event_time_s"])/60:g} min · {choices[0][0]/1000:.2f} km · eligible current endpoint'
        else:
            action=f'Start {tid}: no eligible current endpoint'
            detail='A0 → A20 has a 20-minute gap, above the 12-minute limit.' if current=='A20' else 'Time, distance and speed rules determine eligibility.'
        if phase.startswith('Replay') and current in ['A20','A30']:
            detail+=f' Published assignment changes: trk_A20 → {tid}.'
        b=text(45,48,'How the greedy track resolver builds a journey',28,weight=700)
        b+=text(45,82,'Same reported MMSI · chronological replay · 12-minute link gap · 30-knot speed gate',16,'#61716e')
        b+=rect(45,105,1110,65,'#e2f4f3')+text(65,132,f'{phase}  ·  position {step+1}/{len(order)}',17,'#087f8c',700)
        b+=text(65,156,'Late A10 is inserted at minute 10; mutable assignments are recalculated.' if phase.startswith('Replay') else 'A10 has not arrived. A0 and A20 cannot connect across the gap.',16)
        for yy,v in [(240,'A'),(375,'B')]:
            b+=line(140,yy,1030,yy)+text(65,yy+7,v,24,weight=700)
        for t in [0,10,20,30]:b+=text(150+29*t,440,f'{t} min',16,'#61716e',anchor='middle')
        groups=defaultdict(list)
        for n in names:groups[a[n]['track_id']].append(n)
        for track,ns_ in groups.items():
            for p,q in zip(ns_,ns_[1:]):b+=line(*xy(p),*xy(q),color(track),5)
        for d,p,tr in choices:b+=line(*xy(p),*xy(current),'#172321',2,'5 6')
        for n in P:
            x,y=xy(n);done=n in names;absent=n=='A10' and phase=='Initial reports'
            c=color(a[n]['track_id']) if done else '#cbd7d3'
            if done and endpoints.get(a[n]['track_id'])==n:
                b+=f'<circle cx="{x}" cy="{y}" r="17" fill="none" stroke="#61716e" stroke-width="2"/>'
            if n==current:b+=f'<circle cx="{x}" cy="{y}" r="23" fill="none" stroke="#172321" stroke-width="3"/>'
            b+=f'<circle cx="{x}" cy="{y}" r="9" fill="{c if done else "white"}" stroke="{c}" stroke-width="3" stroke-dasharray="{"3 3" if absent else ""}"/>'
            b+=text(x,y-30,n+(' delayed' if absent else ''),16,'#172321' if done else '#61716e',600,'middle')
            if done:b+=text(x,y+34,a[n]['track_id'],15,color(a[n]['track_id']),600,'middle')
        b+=rect(45,475,1110,105)+text(65,509,action,21,weight=700)
        for j,part in enumerate(textwrap.wrap(detail,width=115)):
            b+=text(65,539+j*23,part,16,'#61716e')
        b+=text(45,600,'Outer ring: current position  ·  thin ring: previous endpoint  ·  dashed line: eligible candidate',15,'#61716e')
        b+=text(45,627,'Timeline layout is schematic. Assignment decisions run the notebook’s resolve_group function.',15,'#61716e')
        s=f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 655" role="img"><title>{esc(action)}</title><rect width="1200" height="655" fill="white"/><g font-family="Arial,Helvetica,sans-serif">{b}</g></svg>'
        frames.append(dict(svg=s,title=action,phase=phase,current=current,assignments=a))
(OUT/'07-greedy-resolver.svg').write_text(frames[-1]['svg'])
with tempfile.TemporaryDirectory() as tmp:
    imgs=[]
    for i,f in enumerate(frames):
        src=Path(tmp)/f'{i}.svg';dst=Path(tmp)/f'{i}.png';src.write_text(f['svg'])
        subprocess.run(['rsvg-convert','-w','1200','-o',str(dst),str(src)],check=True)
        im=Image.open(dst).convert('RGB');imgs.append(im.copy())
    imgs[-1].save(OUT/'07-greedy-resolver.png')
    imgs[0].save(OUT/'07-greedy-resolver.gif',save_all=True,append_images=imgs[1:],duration=[2000]*6+[3500]+[2000]*7+[5000],loop=0,optimize=True)
page='''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AIS track resolver walkthrough</title><style>*{box-sizing:border-box}body{background:#f5f8f7;color:#172321;font:17px Arial,sans-serif;margin:0;padding:24px}main{max-width:1200px;margin:auto}h1{font-size:clamp(24px,4vw,36px)}p{line-height:1.6}button,a{color:#087f8c}button{padding:10px 16px;border:1px solid #bed6d0;border-radius:8px;background:white;cursor:pointer}nav{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin:20px 0}#scene svg{display:block;width:100%;height:auto;border-radius:16px}input{flex:1;min-width:100px}#step{min-width:65px}a:focus-visible,button:focus-visible,input:focus-visible{outline:3px solid #cb6329}</style><main><h1>Watch reports become tracks</h1><p>Step through two vessels reporting the same MMSI. First A10 is delayed, leaving A0 separate from A20/A30. Then A10 arrives and we replay observation time: A20 and A30 acquire A0’s track ID. Each frame was computed using the resolver function from notebook 1.</p><nav aria-label="Animation controls"><button id="prev">Previous</button><button id="play">Play</button><button id="next">Next</button><input id="seek" aria-label="Resolver step" type="range" min="0" max="14" value="0"><span id="step" aria-live="polite"></span></nav><div id="scene"></div><p><a href="07-greedy-resolver.gif">Looping GIF</a> · <a href="07-greedy-resolver.svg">Final frame SVG</a> · <a href="../../notebooks/01_identity_tracks_and_late_data.py">Notebook source</a></p><p>At each step, the resolver considers eligible links from current endpoints, extends the nearest one or starts a segment. The notebook also leaves near-tied choices unassigned; the two routes shown here produce unambiguous choices. At the end of each replay, multi-position segments are marked supported and singletons tentative.</p></main><script>const frames=FRAMES;let i=0,timer;const scene=document.getElementById('scene'),seek=document.getElementById('seek'),step=document.getElementById('step'),play=document.getElementById('play');function show(n){i=Math.max(0,Math.min(frames.length-1,n));scene.innerHTML=frames[i].svg;seek.value=i;step.textContent=(i+1)+' / '+frames.length}function stop(){clearInterval(timer);timer=null;play.textContent='Play'}document.getElementById('prev').onclick=()=>{stop();show(i-1)};document.getElementById('next').onclick=()=>{stop();show(i+1)};seek.oninput=()=>{stop();show(+seek.value)};play.onclick=()=>{if(timer){stop();return}if(i===frames.length-1)show(0);play.textContent='Pause';timer=setInterval(()=>{if(i===frames.length-1){stop();return}show(i+1)},2400)};show(0);</script></html>'''.replace('FRAMES',json.dumps([dict(svg=f['svg']) for f in frames]))
(OUT/'07-greedy-resolver.html').write_text(page)
# Retain trace evidence and deterministic source for rebuilding the illustration.
(OUT/'07-greedy-resolver-steps.json').write_text(json.dumps([{k:v for k,v in f.items() if k!='svg'} for f in frames],indent=2))
print('Built 15 notebook-driven frames; late-bridge assignment and support assertions passed.')
