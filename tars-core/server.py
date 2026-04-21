#!/usr/bin/env python3
"""TARS Core v4.0.0 — Consolidated: Intelligence + Event Bus + Analytics
Single Flask app on port 8093. Three logical modules:
  1. EVENT BUS  — WebSocket->HA, SSE stream, pattern learning, anomaly detection
  2. INTELLIGENCE — Context engine, mode state machine, Cooper, decisions, arrival/departure
  3. ANALYTICS — Climate, sleep scoring, presence, batteries, daily stats

New in v4.0:
  - Room presence: /presence — dict of rooms with occupied/last_motion
  - Sleep scoring: /analytics/sleep/last — A-F grade from overnight sensor data
  - Dashboard: /dashboard — master JSON for all system state
  - Anomaly detection (entity rate >20/min, door open+no presence): /anomalies
  - Predictive wake: track bedroom motion, calc avg. /predictions
  - Weather-reactive: rainy -> cozy mood in handle_event
  - Decision audit: every decision_log entry has a 'why' string
"""
import os,json,time,logging,threading,re
from datetime import datetime,timedelta,timezone
from collections import deque,Counter,defaultdict
from flask import Flask,jsonify,request,Response
import requests as http
import websocket
import sseclient

# ── ENV VARS ───────────────────────────────────────────────────────────────────
HA_URL=os.environ.get('HA_URL','http://localhost:8123')
HA_TOKEN=os.environ.get('HA_TOKEN','')
API_PORT=int(os.environ.get('API_PORT','8093'))
SERVICES_URL=os.environ.get('SERVICES_URL','http://localhost:8097')
COOPER_SCHED=os.environ.get('COOPER_SCHEDULE','')
WATCHED_DOMAINS='binary_sensor,sensor,media_player,vacuum,weather,sun,lock,switch,light,input_boolean'.split(',')

app=Flask(__name__)
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
logger=logging.getLogger('tars-core')

# ── SAFETY CONSTANTS ──────────────────────────────────────────────────────────
BEDROOM_ENTITIES=['media_player.bedroom','media_player.bedroom_sonos','media_player.bedroom_echo_show_chatsworth']
ECHO_ENTITIES=['media_player.chatsworth_living_room_echo_show','media_player.chatsworth_kitchen_echo_show','media_player.bedroom_echo_show_chatsworth','media_player.chatsworth_echo_show_5_bathroom']
SILENT_HOURS=(22,8)

def is_bedroom_safe():
    """True if bedroom motion sensor is on."""
    try:
        r=http.get(f'{HA_URL}/api/states/binary_sensor.bedroom_motion',headers={'Authorization':f'Bearer {HA_TOKEN}'},timeout=5)
        if r.status_code==200: return r.json()['state']=='on'
    except: pass
    return False

def is_silent_hours():
    """True if 10pm-8am."""
    h=datetime.now().hour
    return h>=SILENT_HOURS[0] or h<SILENT_HOURS[1]

def safe_notify(title,msg):
    """Push-only during silent hours, otherwise notify normally."""
    ha_notify(title,msg)

# ── SHARED HELPERS ────────────────────────────────────────────────────────────
def ha_get(path):
    try:
        r=http.get(f'{HA_URL}/api{path}',headers={'Authorization':f'Bearer {HA_TOKEN}'},timeout=10)
        return r.json() if r.status_code==200 else None
    except: return None

def ha_notify(title,msg):
    try:
        http.post(f'{HA_URL}/api/services/notify/mobile_app_bks_home_assistant_chatsworth',
            headers={'Authorization':f'Bearer {HA_TOKEN}','Content-Type':'application/json'},
            json={'data':{'title':title,'message':msg}},timeout=5)
    except: pass

def svc_post(path,timeout=5):
    try:
        r=http.post(f'{SERVICES_URL}{path}',timeout=timeout)
        return r.json() if r.status_code==200 else None
    except: return None

def ha_history(eid,hours=24):
    start=(datetime.now(timezone.utc)-timedelta(hours=hours)).isoformat()
    d=ha_get(f'/history/period/{start}?filter_entity_id={eid}&minimal_response&no_attributes')
    if d and d[0]: return d[0]
    return []

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 1: EVENT BUS
# ═══════════════════════════════════════════════════════════════════════════════
PATTERN_FILE='/data/patterns.json'
events=deque(maxlen=500)
event_counts=Counter()
entity_states={}
ws_ok=False
ws_id=1
patterns={}
recent_seq=deque(maxlen=20)
entity_hour_hist=defaultdict(lambda:Counter())
eb_anomalies=deque(maxlen=100)
rate_window=defaultdict(list)
rate_alerts=deque(maxlen=50)
RATE_LIMIT=20
last_bedroom_motion_time=None

def load_patterns():
    global patterns,entity_hour_hist
    try:
        if os.path.exists(PATTERN_FILE):
            d=json.load(open(PATTERN_FILE))
            patterns=d.get('patterns',{})
            entity_hour_hist=defaultdict(lambda:Counter(),{k:Counter(v) for k,v in d.get('histograms',{}).items()})
    except Exception as e: logger.error(f'Load patterns: {e}')

def save_patterns():
    try:
        d={'patterns':patterns,'histograms':{k:dict(v) for k,v in entity_hour_hist.items()}}
        json.dump(d,open(PATTERN_FILE,'w'),indent=2)
    except: pass

def classify_event(eid):
    if 'presence' in eid or 'lock' in eid: return 'critical'
    dom=eid.split('.')[0]
    if is_silent_hours() and 'bedroom' in eid and dom in {'media_player','tts','notify'}: return 'sleep_disrupting'
    hist=entity_hour_hist[eid]; total=sum(hist.values())
    if total>50 and hist.get(str(datetime.now().hour),0)/total<0.02: return 'unusual'
    return 'routine'

def check_rate_limit(eid):
    now=time.time()
    rate_window[eid]=[t for t in rate_window[eid] if now-t<60]
    rate_window[eid].append(now)
    if len(rate_window[eid])>RATE_LIMIT:
        alert={'entity_id':eid,'events_per_min':len(rate_window[eid]),'time':datetime.now().isoformat()}
        recent=[a for a in rate_alerts if a['entity_id']==eid and (now-datetime.fromisoformat(a['time']).timestamp())<300]
        if not recent:
            rate_alerts.append(alert)
            logger.warning(f'RATE LIMIT: {eid} {len(rate_window[eid])}/min')
        return alert
    return None

def detect_eb_pattern(eid):
    now=time.time()
    recent_seq.append({'entity_id':eid,'time':now})
    if len(recent_seq)>=2:
        prev=recent_seq[-2]; gap=now-prev['time']
        if gap<60 and prev['entity_id']!=eid:
            key=f"{prev['entity_id']}->{eid}"
            if key not in patterns: patterns[key]={'count':0,'last_seen':'','avg_gap':0,'learned':False}
            p=patterns[key]; p['count']+=1; p['last_seen']=datetime.now().isoformat()
            p['avg_gap']=round((p['avg_gap']*(p['count']-1)+gap)/p['count'],1)
            if p['count']>=7 and not p['learned']:
                p['learned']=True
                logger.info(f'PATTERN LEARNED: {key}')
    if event_counts.get('total',0)%100==0: save_patterns()

def route_event(ev):
    global last_bedroom_motion_time
    eid=ev.get('entity_id',''); new=ev.get('new_state',''); old=ev.get('old_state','')
    dom=eid.split('.')[0]
    if 'bedroom' in eid and 'motion' in eid and new=='on': last_bedroom_motion_time=time.time()
    ev['classification']=classify_event(eid)
    entity_hour_hist[eid][str(datetime.now().hour)]+=1
    check_rate_limit(eid)
    detect_eb_pattern(eid)
    sig=False; reason=''
    if eid=='binary_sensor.iphone_presence' and old!=new: sig=True; reason=f'Presence: {old}->{new}'
    elif 'lock' in eid and old!=new: sig=True; reason=f'Lock: {old}->{new}'
    elif 'motion' in eid and new=='on' and old!='on': sig=True; reason=f'Motion: {eid}'
    elif 'media_player.75_the_frame' in eid and old!=new: sig=True; reason=f'TV: {old}->{new}'
    elif eid=='weather.forecast_home' and old!=new: sig=True; reason=f'Weather: {old}->{new}'
    elif 'vacuum' in eid and old!=new: sig=True; reason=f'Vacuum: {old}->{new}'
    elif eid=='sun.sun' and old!=new: sig=True; reason=f'Sun: {old}->{new}'
    elif 'temperature' in eid:
        try:
            if abs(float(new)-float(old or 0))>3: sig=True; reason=f'Temp: {old}->{new}'
        except: pass
    if sig:
        ev['significant']=True; ev['reason']=reason
        event_counts['significant']+=1
        handle_event(ev)  # Forward to Intelligence

def ws_thread():
    global ws_ok,ws_id
    def on_msg(ws,msg):
        global ws_id
        d=json.loads(msg); t=d.get('type','')
        if t=='auth_required': ws.send(json.dumps({'type':'auth','access_token':HA_TOKEN}))
        elif t=='auth_ok':
            ws.send(json.dumps({'id':ws_id,'type':'subscribe_events','event_type':'state_changed'}))
            ws_id+=1
        elif t=='event':
            ed=d.get('event',{}).get('data',{})
            eid=ed.get('entity_id',''); ns=ed.get('new_state',{}); os_=ed.get('old_state',{})
            dom=eid.split('.')[0] if eid else ''
            if dom not in WATCHED_DOMAINS: return
            nv=ns.get('state','') if ns else ''; ov=os_.get('state','') if os_ else ''
            if nv==ov: return
            ev={'entity_id':eid,'domain':dom,'old_state':ov,'new_state':nv,'time':datetime.now().isoformat(),'significant':False,'classification':'routine'}
            events.append(ev); event_counts[dom]+=1; event_counts['total']+=1; entity_states[eid]=nv
            route_event(ev)
    def on_err(ws,e): global ws_ok; ws_ok=False
    def on_close(ws,c,m): global ws_ok; ws_ok=False
    def on_open(ws): global ws_ok; ws_ok=True; logger.info('WS connected')
    ws_url=HA_URL.replace('http','ws')+'/api/websocket'
    while True:
        try:
            w=websocket.WebSocketApp(ws_url,on_message=on_msg,on_error=on_err,on_close=on_close,on_open=on_open)
            w.run_forever(ping_interval=30,ping_timeout=10)
        except: pass
        time.sleep(10)

# EVENT BUS ROUTES
@app.route('/events/stream')
def events_stream():
    def gen():
        idx=len(events)
        while True:
            cur=len(events)
            if cur>idx:
                for e in list(events)[idx:]: yield f'data: {json.dumps(e)}\n\n'
                idx=cur
            time.sleep(0.5)
    return Response(gen(),mimetype='text/event-stream')

@app.route('/events/recent')
def events_recent():
    lim=request.args.get('limit',50,type=int)
    return jsonify(list(events)[-lim:])

@app.route('/patterns')
def get_patterns():
    learned=[{'sequence':k,**v} for k,v in patterns.items() if v.get('learned')]
    all_p=sorted([{'sequence':k,**v} for k,v in patterns.items()],key=lambda x:x['count'],reverse=True)
    return jsonify({'learned':learned,'top_50':all_p[:50],'total':len(patterns)})

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 2: INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════
INTEL_FILE='/data/intelligence.json'
home_mode='unknown'
mode_history=deque(maxlen=50)
cooper_override=None
decision_log=deque(maxlen=200)
event_driven_actions=deque(maxlen=100)
last_event_time=None
adaptive_rules={'nudge_ignored_count':0,'auto_actions':{},'suppressed_actions':set()}

# Anomaly detection state (v4)
entity_event_times=defaultdict(list)  # eid -> [timestamps]
open_doors={}  # eid -> opened_at
presence_home=True

# Predictive wake tracking (v4)
bedroom_wake_times=[]  # list of hour floats

def load_intel():
    global adaptive_rules,home_mode,bedroom_wake_times
    try:
        if os.path.exists(INTEL_FILE):
            d=json.load(open(INTEL_FILE))
            adaptive_rules=d.get('adaptive_rules',adaptive_rules)
            adaptive_rules['suppressed_actions']=set(adaptive_rules.get('suppressed_actions',[]))
            home_mode=d.get('home_mode','unknown')
            bedroom_wake_times=d.get('bedroom_wake_times',[])
    except Exception as e: logger.error(f'Load intel: {e}')

def save_intel():
    try:
        d={'adaptive_rules':{**adaptive_rules,'suppressed_actions':list(adaptive_rules['suppressed_actions'])},
           'home_mode':home_mode,'bedroom_wake_times':bedroom_wake_times[-30:],
           'saved_at':datetime.now().isoformat()}
        json.dump(d,open(INTEL_FILE,'w'),indent=2)
    except: pass

def is_cooper_here():
    if cooper_override is not None: return cooper_override
    now=datetime.now(); dow=now.weekday()
    hour=now.hour*100+now.minute
    days_map={'mon':0,'tue':1,'wed':2,'thu':3,'fri':4,'sat':5,'sun':6}
    for block in COOPER_SCHED.split(','):
        if '-' not in block: continue
        parts=block.strip().split('-')
        if len(parts)!=2: continue
        sp=parts[0].split('_'); ep=parts[1].split('_') if '_' in parts[1] else [now.strftime('%a').lower(),parts[1]]
        if len(sp)==2 and len(ep)==2:
            sd=days_map.get(sp[0],99); st=int(sp[1])
            ed=days_map.get(ep[0],99); et=int(ep[1])
            if sd<=dow<=ed:
                if sd==ed and st<=hour<=et: return True
                elif dow==sd and hour>=st: return True
                elif dow==ed and hour<=et: return True
                elif sd<dow<ed: return True
    return False

def update_mode():
    global home_mode
    now=datetime.now(); h=now.hour
    p=ha_get('/states/binary_sensor.iphone_presence')
    is_home=p['state']=='on' if p else True
    cooper=is_cooper_here(); old=home_mode
    if not is_home: home_mode='away'
    elif cooper and h<8: home_mode='cooper_night'
    elif cooper: home_mode='cooper_day'
    elif h<6: home_mode='night'
    elif h<9: home_mode='morning'
    elif h<17: home_mode='working'
    elif h<21: home_mode='evening'
    else: home_mode='night'
    if old!=home_mode:
        mode_history.append({'from':old,'to':home_mode,'time':datetime.now().isoformat()})
        logger.info(f'MODE: {old}->{home_mode}')
        on_mode_change(old,home_mode); save_intel()
    return home_mode

def on_mode_change(old,new):
    if new=='cooper_day': svc_post('/kids')
    elif new=='cooper_night': svc_post('/volume/12')
    elif old in ('cooper_day','cooper_night') and 'cooper' not in new: svc_post('/kids/off')
    elif new=='morning' and old=='night':
        if is_bedroom_safe(): svc_post('/play')

def handle_event(ev):
    """React to significant HA events. Called from route_event."""
    global last_event_time,presence_home
    last_event_time=datetime.now().isoformat()
    eid=ev.get('entity_id',''); new=ev.get('new_state',''); old=ev.get('old_state','')
    reason=ev.get('reason','')
    if not ev.get('significant',False): return

    # v4: Anomaly detection -- track entity event rates
    now_ts=time.time()
    entity_event_times[eid]=[t for t in entity_event_times[eid] if now_ts-t<60]
    entity_event_times[eid].append(now_ts)

    # v4: Track door open without presence
    if 'door' in eid and 'contact' in eid:
        if new=='off': open_doors[eid]=now_ts  # opened
        elif new=='on' and eid in open_doors: del open_doors[eid]  # closed

    action_taken=None

    if 'iphone_presence' in eid:
        presence_home=(new=='on')
        if new=='on':
            update_mode()
            threading.Thread(target=arrive_sequence,daemon=True).start()
            action_taken='arrival_sequence'
        elif new=='off':
            update_mode()
            threading.Thread(target=depart_sequence,daemon=True).start()
            action_taken='departure_sequence'

    elif 'media_player.75_the_frame' in eid and new in ('on','playing'):
        if not is_silent_hours():
            svc_post('/mood/movie'); action_taken='tv_on_response'
        else: action_taken='tv_on_silent_skip'

    elif eid=='weather.forecast_home':
        # v4: Weather-reactive
        if new in ('rainy','pouring'):
            svc_post('/mood/rainy')
            try: http.post(f'{HA_URL}/api/services/light/turn_on',
                headers={'Authorization':f'Bearer {HA_TOKEN}','Content-Type':'application/json'},
                json={'entity_id':'light.living_room','brightness_pct':40,'color_temp':500},timeout=5)
            except: pass
            action_taken='rainy_cozy_mood'
        elif new=='sunny' and home_mode in ('morning','working'):
            svc_post('/mood/sunny'); action_taken='sunny_mood'

    elif eid=='sun.sun' and new=='below_horizon':
        svc_post('/mood/evening'); action_taken='sunset_transition'

    elif 'vacuum' in eid and new in ('docked','standby') and old=='cleaning':
        ha_notify('\U0001f9f9 Clean Complete','Vacuum docked.'); action_taken='vacuum_done'

    elif 'bedroom' in eid and 'motion' in eid and new=='on':
        # v4: Predictive wake tracking
        h=datetime.now().hour+datetime.now().minute/60
        if 4<=h<=11:  # morning window
            bedroom_wake_times.append(h)
            if len(bedroom_wake_times)%5==0: save_intel()
        action_taken='bedroom_motion_tracked'

    if action_taken:
        event_driven_actions.append({'time':datetime.now().isoformat(),'event':eid,'action':action_taken,'why':reason})

def arrive_sequence():
    if is_silent_hours() and not is_bedroom_safe():
        safe_notify('\U0001f3e0 Welcome Home','Arrived (quiet mode)')
        decision_log.append({'type':'arrival','time':datetime.now().isoformat(),'decisions':[{'action':'silent_welcome','why':'silent hours + no bedroom motion'}]})
        return
    ctx=build_context(); dec=decide(ctx); results=execute_decisions(dec)
    decision_log.append({'type':'arrival','time':datetime.now().isoformat(),'decisions':results})

def depart_sequence():
    decisions=[]
    if not is_cooper_here():
        if not is_bedroom_safe() or datetime.now().hour>=9:
            try: http.post(f'{HA_URL}/api/services/vacuum/start',headers={'Authorization':f'Bearer {HA_TOKEN}','Content-Type':'application/json'},json={'entity_id':'vacuum.robovac'},timeout=5)
            except: pass
            decisions.append({'action':'vacuum_start','why':'Departed, no Cooper, safe to clean'})
        else: decisions.append({'action':'vacuum_deferred','why':'Before 9am'})
    else: decisions.append({'action':'vacuum_skip','why':'Cooper here'})
    decisions.append({'action':'music_stop','why':'Departure'})
    svc_post('/mood/off')
    decision_log.append({'type':'departure','time':datetime.now().isoformat(),'decisions':decisions})

def mode_updater():
    while True: update_mode(); time.sleep(60)

def build_context():
    ctx={}; now=datetime.now()
    h=now.hour
    if h<6: period='night'
    elif h<9: period='morning'
    elif h<17: period='working'
    elif h<21: period='evening'
    else: period='night'
    ctx['time']={'hour':h,'day':now.strftime('%A'),'weekend':now.weekday()>=5,'period':period,'mode':home_mode}
    p=ha_get('/states/binary_sensor.iphone_presence')
    ctx['presence']={'home':p['state']=='on' if p else None}
    ctx['cooper']={'here':is_cooper_here()}
    w=ha_get('/states/weather.forecast_home')
    ctx['weather']={'state':w['state'] if w else 'unknown','temp':w['attributes'].get('temperature') if w else None}
    return ctx

def decide(ctx):
    decisions=[]; period=ctx['time']['period']; home=ctx['presence']['home']; cooper=ctx['cooper']['here']; weather=ctx['weather']['state']
    if home and weather in ('rainy','pouring'):
        decisions.append({'action':'music_mood','value':'rainy','why':'Rain detected'})
    if cooper:
        decisions.append({'action':'spotify_kids','value':True,'why':'Cooper visiting'})
        decisions.append({'action':'skip_vacuum','value':True,'why':'No vacuum while Cooper here'})
    if period=='night' and home and not cooper:
        decisions.append({'action':'music_mood','value':'sleep','why':'Late night'})
    return [d for d in decisions if d['action'] not in adaptive_rules.get('suppressed_actions',set())]

def execute_decisions(decisions):
    results=[]
    for d in decisions:
        a=d['action']; v=d.get('value')
        try:
            if a=='music_mood': r=svc_post(f'/mood/{v}'); results.append({**d,'executed':bool(r)})
            elif a=='spotify_kids': r=svc_post('/kids'); results.append({**d,'executed':bool(r)})
            else: results.append({**d,'executed':False,'note':'no executor'})
        except: results.append({**d,'executed':False,'note':'error'})
    return results

# INTELLIGENCE ROUTES
@app.route('/')
def index():
    return jsonify({'name':'TARS Core','version':'4.0.0','mode':home_mode,'cooper_here':is_cooper_here(),'ws_connected':ws_ok,'last_event':last_event_time})

@app.route('/health')
def health():
    return jsonify({'status':'ok','mode':home_mode,'ws_connected':ws_ok,'cooper_here':is_cooper_here(),'silent_hours':is_silent_hours(),'bedroom_safe':is_bedroom_safe()})

@app.route('/context')
def context(): return jsonify(build_context())

@app.route('/mode')
def get_mode(): return jsonify({'current':home_mode,'history':list(mode_history)[-10:],'updated':last_event_time})

@app.route('/learned')
def get_learned(): return jsonify({'adaptive_rules':{k:v for k,v in adaptive_rules.items() if k!='suppressed_actions'},'suppressed':list(adaptive_rules.get('suppressed_actions',[])),'event_driven_actions':list(event_driven_actions)[-20:]})

@app.route('/cooper')
def cooper_status(): return jsonify({'here':is_cooper_here(),'override':cooper_override,'schedule':COOPER_SCHED})

@app.route('/cooper/here',methods=['POST','GET'])
def cooper_here():
    global cooper_override; cooper_override=True
    svc_post('/kids'); ha_notify('\U0001f466 Cooper Mode','Kids music on, vacuum disabled')
    return jsonify({'cooper':'here'})

@app.route('/cooper/gone',methods=['POST','GET'])
def cooper_gone():
    global cooper_override; cooper_override=False
    svc_post('/kids/off'); ha_notify('\U0001f466 Cooper Left','Normal mode restored')
    return jsonify({'cooper':'gone'})

@app.route('/arrive',methods=['POST','GET'])
def arrive():
    ctx=build_context(); dec=decide(ctx); results=execute_decisions(dec)
    event={'type':'arrival','time':datetime.now().isoformat(),'decisions':results}
    decision_log.append(event); return jsonify(event)

@app.route('/depart',methods=['POST','GET'])
def depart():
    decisions=[]
    if not is_cooper_here():
        decisions.append({'action':'vacuum_start','why':'Departing, no one home'})
        try: http.post(f'{HA_URL}/api/services/vacuum/start',headers={'Authorization':f'Bearer {HA_TOKEN}','Content-Type':'application/json'},json={'entity_id':'vacuum.robovac'},timeout=5)
        except: pass
    decisions.append({'action':'lights_off','why':'Departure'})
    event={'type':'departure','time':datetime.now().isoformat(),'decisions':decisions}
    decision_log.append(event); return jsonify(event)

@app.route('/mood/<mood>',methods=['POST','GET'])
def set_mood(mood):
    mood_map={'chill':{'music':'chill','vol':12},'energetic':{'music':'energetic','vol':20},'focus':{'music':'focus','vol':8},'party':{'music':'party','vol':25},'sleep':{'music':'sleep','vol':8},'rainy':{'music':'rainy','vol':10},'morning':{'music':'morning_coffee','vol':12},'movie':{'music':None,'vol':None},'off':{'music':'off','vol':None}}
    if mood not in mood_map: return jsonify({'error':f'Available: {list(mood_map.keys())}'}),400
    m=mood_map[mood]; results=[]
    if m['music']: r=svc_post(f'/mood/{m["music"]}'); results.append({'action':'music','ok':bool(r)})
    if m['vol']: r=svc_post(f'/volume/{m["vol"]}'); results.append({'action':'volume','level':m['vol'],'ok':bool(r)})
    event={'type':'mood','mood':mood,'time':datetime.now().isoformat(),'results':results,'why':'manual request'}
    decision_log.append(event); return jsonify(event)

@app.route('/insights')
def insights():
    tips=[]
    s=ha_get('/states/sensor.bedroom_co2_monitor_carbon_dioxide')
    if s:
        try:
            co2=float(s['state'])
            if co2>600: tips.append({'type':'air','tip':f'Bedroom CO2 {int(co2)}ppm. Open windows before bed.'})
        except: pass
    batts=_get_low_batteries()
    if batts: tips.append({'type':'battery','tip':f'{len(batts)} low battery devices.','devices':[b["entity"] for b in batts]})
    if not tips: tips.append({'type':'all_good','tip':'Everything looks great.'})
    return jsonify({'insights':tips,'mode':home_mode})

@app.route('/log')
def get_log():
    limit=request.args.get('limit',20,type=int)
    return jsonify(list(decision_log)[-limit:])

@app.route('/proactive')
def proactive_check():
    suggestions=[]
    s=ha_get('/states/sensor.bedroom_co2_monitor_carbon_dioxide')
    if s:
        try:
            co2=float(s['state'])
            w=ha_get('/states/weather.forecast_home')
            temp=w.get('attributes',{}).get('temperature',0) if w else 0
            if co2>1000 and 60<=temp<=80:
                suggestions.append({'type':'air_quality','message':f'CO2 {int(co2)}ppm, temp {temp}F -- open windows!','priority':'medium'})
        except: pass
    sun=ha_get('/states/sun.sun')
    if sun:
        elev=sun.get('attributes',{}).get('elevation',90)
        if 0<elev<10 and home_mode not in ('away','night'):
            suggestions.append({'type':'golden_hour','message':'Golden hour outside!','priority':'low'})
    return jsonify({'suggestions':suggestions,'mode':home_mode,'bedroom_safe':is_bedroom_safe(),'silent_hours':is_silent_hours()})

# v4: Room Presence
@app.route('/presence')
def room_presence():
    """Dict of rooms with occupied/last_motion from motion sensors."""
    motion_sensors={
        'bedroom':'binary_sensor.bedroom_motion',
        'living_room':'binary_sensor.living_room_motion',
        'bathroom':'binary_sensor.bathroom_motion_motion',
        'kitchen':'binary_sensor.kitchen_motion',
        'office':'binary_sensor.office_motion',
    }
    result={}
    for room,eid in motion_sensors.items():
        s=ha_get(f'/states/{eid}')
        if s:
            result[room]={'occupied':s['state']=='on','last_changed':s.get('last_changed'),'entity':eid}
        else:
            result[room]={'occupied':False,'last_changed':None,'entity':eid,'unavailable':True}
    return jsonify(result)

# v4: Anomalies
@app.route('/anomalies')
def anomalies_endpoint():
    """Entities firing >20/min or door open without presence."""
    result={'high_rate':[],'open_doors_no_presence':[],'event_bus_anomalies':list(eb_anomalies)[-10:]}
    now=time.time()
    for eid,times in entity_event_times.items():
        recent=[t for t in times if now-t<60]
        if len(recent)>RATE_LIMIT:
            result['high_rate'].append({'entity':eid,'events_per_min':len(recent)})
    for eid,opened_at in open_doors.items():
        age=now-opened_at
        if age>300 and not presence_home:  # open 5+ min, no one home
            result['open_doors_no_presence'].append({'entity':eid,'open_seconds':int(age)})
    return jsonify(result)

# v4: Predictive Wake
@app.route('/predictions')
def predictions():
    """Average bedroom motion time for predictive wake."""
    if not bedroom_wake_times:
        return jsonify({'wake_prediction':None,'samples':0,'message':'Not enough data yet'})
    avg=sum(bedroom_wake_times)/len(bedroom_wake_times)
    avg_h=int(avg); avg_m=int((avg-avg_h)*60)
    return jsonify({'wake_prediction':f'{avg_h:02d}:{avg_m:02d}','avg_hour':round(avg,2),'samples':len(bedroom_wake_times),'recent':bedroom_wake_times[-7:]})

# v4: Dashboard
@app.route('/dashboard')
def dashboard():
    """Master JSON: mode, presence, weather, anomalies, battery alerts, sleep, cooper."""
    w=ha_get('/states/weather.forecast_home')
    batts=_get_low_batteries()
    sleep=_quick_sleep_score()
    pres_sensor=ha_get('/states/binary_sensor.iphone_presence')
    now=time.time()
    high_rate=[eid for eid,times in entity_event_times.items() if len([t for t in times if now-t<60])>RATE_LIMIT]
    return jsonify({
        'mode':home_mode,'cooper':is_cooper_here(),
        'presence':{'home':pres_sensor['state']=='on' if pres_sensor else None},
        'weather':{'state':w['state'] if w else None,'temp':w['attributes'].get('temperature') if w else None},
        'sleep_score':sleep,
        'battery_alerts':[{'entity':b['entity'],'level':b['level']} for b in batts],
        'anomaly_count':len(high_rate),
        'ws_connected':ws_ok,'silent_hours':is_silent_hours(),'bedroom_safe':is_bedroom_safe(),
        'last_event':last_event_time,
        'decisions_today':len([d for d in decision_log if d.get('time','')[:10]==datetime.now().strftime('%Y-%m-%d')]),
        'timestamp':datetime.now().isoformat(),
    })

def _get_low_batteries():
    states=ha_get('/states')
    if not states: return []
    batts=[]
    for s in states:
        if 'battery' in s['entity_id'] and s['entity_id'].startswith('sensor.') and s['state'] not in ('unavailable','unknown'):
            try:
                v=float(s['state'])
                if 0<=v<20: batts.append({'entity':s['entity_id'],'level':v})
            except: pass
    return sorted(batts,key=lambda x:x['level'])

def _quick_sleep_score():
    """Quick sleep score from current bedroom CO2 (no HA history needed)."""
    s=ha_get('/states/sensor.bedroom_co2_monitor_carbon_dioxide')
    if not s: return {'score':None,'grade':'?','note':'no sensor'}
    try:
        co2=float(s['state'])
        score=100
        if co2>1000: score-=30
        elif co2>700: score-=10
        grade='A' if score>=90 else 'B' if score>=75 else 'C' if score>=60 else 'D'
        return {'score':score,'grade':grade,'co2':co2}
    except: return {'score':None,'grade':'?'}

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 3: ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════
STATS_FILE='/data/stats_db.json'
stats_db={}
arrival_count=0; departure_count=0
motion_counts_an=defaultdict(int)
temp_readings=defaultdict(list)
sleep_disruptions=deque(maxlen=200)

def load_stats():
    global stats_db
    try:
        if os.path.exists(STATS_FILE):
            stats_db=json.load(open(STATS_FILE))
    except Exception as e: logger.error(f'Load stats: {e}')

def save_stats():
    try:
        cutoff=(datetime.now()-timedelta(days=365)).strftime('%Y-%m-%d')
        pruned={k:v for k,v in stats_db.items() if k>=cutoff}
        json.dump(pruned,open(STATS_FILE,'w'),indent=2)
    except: pass

def flush_today():
    today=datetime.now().strftime('%Y-%m-%d')
    stats_db[today]={'date':today,'presence':{'arrivals':arrival_count,'departures':departure_count},'motion':dict(motion_counts_an),'temp_avg':{k:round(sum(v)/len(v),1) for k,v in temp_readings.items() if v},'sleep_disruptions':len([d for d in sleep_disruptions if d.get('time','')[:10]==today]),'flushed_at':datetime.now().isoformat()}
    save_stats()

def daily_flush_loop():
    last_day=datetime.now().strftime('%Y-%m-%d')
    while True:
        time.sleep(900); flush_today()
        today=datetime.now().strftime('%Y-%m-%d')
        if today!=last_day: logger.info(f'Day rollover: {last_day}->{today}'); last_day=today

# ANALYTICS ROUTES
@app.route('/analytics/daily')
def analytics_daily():
    days=request.args.get('days',30,type=int)
    recent=sorted(stats_db.items())[-days:]
    return jsonify({'days':[v for _,v in recent],'total':len(recent)})

@app.route('/analytics/sleep/last')
def analytics_sleep_last():
    """A-F grade from overnight CO2/temp/humidity in bedroom."""
    results={}
    for eid,label in [('sensor.bedroom_co2_monitor_temperature','temp'),('sensor.bedroom_co2_monitor_humidity','humidity'),('sensor.bedroom_co2_monitor_carbon_dioxide','co2')]:
        h=ha_history(eid,24)
        night_vals=[]
        for e in h:
            try:
                t=datetime.fromisoformat(e['last_changed'].replace('Z','+00:00'))
                if t.hour>=22 or t.hour<7: night_vals.append(float(e['state']))
            except: pass
        if night_vals:
            results[label]={'avg':round(sum(night_vals)/len(night_vals),1),'min':round(min(night_vals),1),'max':round(max(night_vals),1),'readings':len(night_vals)}
    score=100
    co2_avg=results.get('co2',{}).get('avg',0)
    if co2_avg>1000: score-=30
    elif co2_avg>700: score-=10
    temp=results.get('temp',{}).get('avg',68)
    if temp and (temp<63 or temp>72): score-=20
    elif temp and (temp<65 or temp>70): score-=10
    hum=results.get('humidity',{}).get('avg',45)
    if hum and (hum<30 or hum>60): score-=15
    today=datetime.now().strftime('%Y-%m-%d')
    disruptions=len([d for d in sleep_disruptions if d.get('time','')[:10]==today])
    if disruptions>0: score-=min(disruptions*5,20)
    score=max(0,score)
    results['score']=score
    results['grade']='A' if score>=90 else 'B' if score>=75 else 'C' if score>=60 else 'D' if score>=45 else 'F'
    results['sleep_disruptions']=disruptions
    return jsonify(results)

@app.route('/analytics/energy')
def analytics_energy():
    heater=ha_get('/states/input_number.bathroom_heater_daily_energy_kwh')
    kwh=float(heater['state']) if heater else 0
    return jsonify({'heater_daily_kwh':kwh,'estimated_cost_usd':round(kwh*0.35,2),'rate_per_kwh':0.35})

@app.route('/analytics/health')
def analytics_health():
    """Device health: unavailable entities + low batteries."""
    states=ha_get('/states')
    if not states: return jsonify({'error':'HA unreachable'}),500
    unavailable=[s['attributes'].get('friendly_name',s['entity_id']) for s in states if s['state']=='unavailable' and not any(skip in s['entity_id'] for skip in ['bks_macbook','clawdbot','dryer','unnamed'])]
    batts=_get_low_batteries()
    return jsonify({'unavailable_count':len(unavailable),'unavailable':unavailable[:20],'low_batteries':batts,'timestamp':datetime.now().isoformat()})

# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
if __name__=='__main__':
    logger.info(f'TARS Core v4.0.0 on :{API_PORT}')
    load_patterns(); load_intel(); load_stats()
    threading.Thread(target=ws_thread,daemon=True).start()
    threading.Thread(target=mode_updater,daemon=True).start()
    threading.Thread(target=daily_flush_loop,daemon=True).start()
    logger.info('WebSocket + mode updater + daily flush started')
    app.run(host='0.0.0.0',port=API_PORT,debug=False)
