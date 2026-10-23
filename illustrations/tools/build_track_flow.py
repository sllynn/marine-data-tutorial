"""Render the reception-to-track flow diagram (requires rsvg-convert)."""
from pathlib import Path
import html,subprocess
out=Path(__file__).resolve().parents[1]/'identity'
def text(x,y,s,size=18,color='#172321',weight=400):
 return f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" font-weight="{weight}">{html.escape(s)}</text>'
b=text(40,48,'From received messages to assigned positions',28,weight=700)
b+=text(40,82,'Bronze preserves receptions. Silver publishes the current track decision for each accepted observation.',17,'#61716e')
cards=[(40,'AIS feed','Position reports', ['A0 · receiver 1','A0 · receiver 2','A20 · receiver 1']),
 (345,'Bronze','Every reception',['obs_id · MMSI · time','LAT · LON · source','Reported vessel fields']),
 (650,'Track assignment','foreachBatch',['Read in-scope Silver','H3 + movement checks','Resolve each MMSI']),
 (955,'Silver','Accepted positions',['obs_id · MMSI · time','LAT · LON · sources','track_id · track_status'])]
for i,(x,title,subtitle,rows) in enumerate(cards):
 b+=f'<g id="stage-{i}"><title>{title}</title><rect x="{x}" y="125" width="265" height="235" rx="16" fill="{"#e2f4f3" if i==3 else "#f5f8f7"}"/>'
 b+=text(x+20,162,title,23,weight=700)+text(x+20,192,subtitle,17,'#087f8c',600)
 for j,r in enumerate(rows):b+=text(x+20,237+j*30,r,16)
 b+='</g>'
 if i<3:b+=f'<path d="M{x+272} 240 h25" stroke="#087f8c" stroke-width="3" marker-end="url(#arrow)"/>'
b+=f'<path d="M1085 370 V415 H785 V370" fill="none" stroke="#087f8c" stroke-width="2.5" marker-end="url(#arrow)"/>'
b+=text(810,445,'History feeds the next replay',17,'#087f8c',600)
b+=text(40,410,'Repeated receptions share obs_id.',18,weight=600)+text(40,443,'A0 contributes one Silver position, with both receivers retained.',17,'#61716e')
b+=text(40,502,'When late A10 arrives, MERGE can update A20 and A30’s track_id as well as insert A10.',18)
s=f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1260 540" role="img" aria-labelledby="title desc"><title id="title">AIS feed to Bronze, track assignment and Silver</title><desc id="desc">Four stages preserve every reception in Bronze, then deduplicate and resolve tracks using relevant Silver history, publishing accepted positions and revisable track IDs in Silver.</desc><defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0 0 L6 3 L0 6" fill="none" stroke="#087f8c" stroke-width="1.5"/></marker></defs><rect width="1260" height="540" fill="white"/><g font-family="Arial,Helvetica,sans-serif">{b}</g></svg>'
p=out/'06-receptions-to-tracks.svg';p.write_text(s)
subprocess.run(['rsvg-convert','-w','2400','-o',str(p.with_suffix('.png')),str(p)],check=True)
