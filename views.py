#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import flask
import os, bcrypt
import numpy as np
import time
import sys
from .forms import LoginForm
from .models import User
from functools import wraps
from flask.ext.login import login_user, logout_user, current_user, login_required
from flask import redirect, url_for, make_response, flash, g
from flask import render_template
from flask import request, send_from_directory
from fantasticsearch import fantasticsearch, db, lm
from collections import Counter
from elasticsearch import Elasticsearch
sys.path.append("/usr/lib/python2.7/dist-packages")
user = "regalmed\\TRodriguez"
import mechanize
from bs4 import BeautifulSoup
from ntlm import HTTPNtlmAuthHandler
from datetime import datetime


#TODO: Configuration

reload(sys)
sys.setdefaultencoding('utf-8')
host = "http://localhost:9200"
tsting = False 
indexName = ["rss", "ath", "rxx", "lab", "clm", "nts"] 
mm_idx = "mem"
aggregationFields = ["pharmacy", "labs", "diag", "cpt", "spclt", "db", "tags", "link", "year"]


es = Elasticsearch(host)

TAG_RE = re.compile(r'<[^>]+>')

def remove_tags(text):
    return TAG_RE.sub('', text)

################################

@fantasticsearch.route('/timeline', methods=['GET', 'POST'])
def timeline():
    return send_from_directory(directory=fantasticsearch.static_folder, filename='linegraph.csv') 

@fantasticsearch.route('/sunburst', methods=['GET', 'POST'])
def sunburst():
    return send_from_directory(directory=fantasticsearch.static_folder, filename='sunburst.json') 

@fantasticsearch.route('/data', methods=['GET', 'POST'])
def data():
    return send_from_directory(directory=fantasticsearch.static_folder, filename='data.tsv') 

@fantasticsearch.route('/data1', methods=['GET', 'POST'])
def data1():
    return send_from_directory(directory=fantasticsearch.static_folder, filename='data1.tsv') 

@fantasticsearch.route('/rev_dump', methods=['GET', 'POST'])
def rev_dump():
    return send_from_directory(directory=fantasticsearch.static_folder, filename='yelp_clinic_links.csv') 

@fantasticsearch.before_request
def before_request():
    g.user = current_user

@lm.user_loader
def user_loader(user_id):
    """Given *user_id*, return the associated User object.
    :param unicode user_id: user_id (email) user to retrieve
    """
    return User.query.get(user_id)

@fantasticsearch.route('/scoutdash')
@login_required
def scoutdash():
    """Run and display various analytics reports."""
    filters = request.args.get('filter', '')
    page = request.args.get('page', '')
    term = "*"
    results = performQuery(term, filters, page)
    tz = rndr(results)
    return render_template('scout_dash.html', tz=tz)

@fantasticsearch.route("/login", methods=["GET", "POST"])
def login():
    """For GET requests, display the login form. For POSTS, login the current user
    by processing the form."""
    print db
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.get(form.email.data)
        if user:
            if bcrypt.hashpw(str(form.password.data), str(user.password)) == str(user.password):
                user.authenticated = True
                db.session.add(user)
                db.session.commit()
                login_user(user, remember=True)
                return redirect(url_for("scoutdash"))
    return render_template("login.html", form=form)

@fantasticsearch.route("/logout", methods=["GET"])
@login_required
def logout():
    """Logout the current user."""
    user = current_user
    user.authenticated = False
    db.session.add(user)
    db.session.commit()
    logout_user()
    return render_template("logout.html")

#####################################################################################
@fantasticsearch.route('/grapevine')
def grapevine():
    rez = es.search(index='rep3', doc_type="review", body={"query": {"match_all": {}}, "size":1})
    rr = rez['hits']['hits'][0]['_source']
    main_categories = ["wait","rude","service","phone","appointment","letter"]
    cat_lst= []
    for ll in main_categories:
        if rr[ll] == 1:
            cat_lst.append(ll)
    resp = make_response(render_template('grapevine_index.html', rez=rez, cat_lst=cat_lst, page=1))
    return resp

@fantasticsearch.route('/grape_next')
def grape_next():
    term = "*"
    # Collecting info to index in new index
    review_id = request.args.get('cd_id', '')
    response = es.get(index="rep3", doc_type="review",id=review_id)
    name = str(response['_source']['name'])
    text = str(response['_source']['text'])
    entity = str(response['_source']['entity'])
    date = str(response['_source']['date'])
    stars = response['_source']['stars']
    link = str(response['_source']['link'])
    path = str(response['_source']['path'])
    bd = {"name":name, "text":text,"entity":entity, "date":date, "stars":stars, "link": link, "path":path}
    es.index(index="rep_done", doc_type='review', id=review_id, body=bd)
    page = request.args.get('page', '')
    es.delete(index="rep3", doc_type="review", id=review_id, refresh=True)
    rez = es.search(index="rep3", doc_type="review", body={"query": {"match_all": {}}, "size":1})
    resp = make_response(render_template('grapevine_index.html', rez=rez, term=term, page=page))
    return resp


def rand_sent(rez):
    rr = [str(x['_source']['text']) for x in rez]
    NN = np.random.randint(len(rr))
    pp = rr[NN].replace('Dr.', 'Dr').split('.')
    qq = pp[np.random.randint(len(pp))]
    qq = qq.split()
    MM = len(qq)
    qq = qq[:20]
    if len(qq) < MM:
        qq.append('. . .')
    elif MM > 1:
        qq[-1] = qq[-1] + '.'
    qq = ' '.join(qq)
    return remove_tags(qq)
    

@fantasticsearch.route('/grapedash')
def grapedash():
    rez = es.search(index='rep3', doc_type="review", body={"query": {"match_all": {}},"size":1000})['hits']['hits']
    qq = ''
    while len(qq) < 20:
        qq = rand_sent(rez)
    resp = make_response(render_template('grapedash_index.html', qq=qq, page=1))
    return resp
##################################################################################

def clm_flt(x):
    try:
        x['_source']['db'] == 'Claim'
        return False
    except KeyError:
        return True

@fantasticsearch.route('/auth_lookup/<path:ath_no>/', methods=['GET', 'PUT'])
def auth_lookup(ath_no):
    ath_query = {"query": {"match": {"nm": ath_no}}, "size": 500}
    res = es.search(index=indexName, doc_type="supp", body=ath_query)['hits']['hits']
    res = [x for x in res if clm_flt(x) == True]
    lnk_lst = list(set([x['_source']['link'] for x in res]))
    if len(lnk_lst) == 1:
        return send_from_directory(directory=fantasticsearch.static_folder, filename='PS/' + lnk_lst[0].strip('/home/'))
    if len(lnk_lst) > 1:
        return render_template('hplan.html', lnk_lst=lnk_lst, msg='')
    if len(lnk_lst) == 0:
        return render_template('hplan.html', lnk_lst=lnk_lst, msg='No Attachments')


@fantasticsearch.route('/dofr/<path:company>/<path:hpcode>', methods=['GET', 'POST'])
def dofr(company, hpcode):
    tm_stp = time.strftime('%Y_%m_%d')
    hpcode = request.cookies.get('hpcode').encode('utf-8').replace('(', '_').replace(')', '_').replace('\\', '_').replace('-','_').replace('/', '_').replace(',','_').replace('&','_')
    company = request.cookies.get('company').encode('utf-8')
    flg = False
    for rt, dr, fl in os.walk('/home/trodriguez/Documents/search/DOFR/dofr_html'):
        for f in fl:
            if company in f and hpcode in f:
                return render_template('DF/' + f)
                flg = True
                break
    if flg == False:
        return redirect('http://portal/regalnet/ClmResources.cfm')

@fantasticsearch.route('/hplanlookup/<path:hpcode>/<path:opt>', methods=['GET', 'POST'])
def hplan(hpcode, opt):
    opt = request.cookies.get('opt').encode('utf-8')
    hpcode = request.cookies.get('hpcode').encode('utf-8')
    url = "http://portal/regalnet/DMHealthOptions.cfm"
    try:
        return render_template('BF/' + hpcode + '_' + opt + '.html')
    except:
        pass_manager = mechanize.HTTPPasswordMgrWithDefaultRealm()
        pass_manager.add_password(None, url, user, password)
        auth_NTLM = HTTPNtlmAuthHandler.HTTPNtlmAuthHandler(pass_manager)
        browser = mechanize.Browser(factory=mechanize.RobustFactory())
        browser.set_handle_robots(False)
        browser.add_handler(auth_NTLM)
        r = browser.open(url)
        browser.select_form(name="HPlanLookup")
        browser["txtSearch"] = hpcode 
        browser["txtOPT"] = opt
        res = browser.submit()
        soup = BeautifulSoup(res.read())
        if 'Sorry, No records' in soup.findAll('p')[-1].text:
            return redirect(url)
        else:
            for th in soup.findAll('th'):
                th.replaceWith('')
            tbl = soup.findAll("table")[2]
            tbl['class'] = "highlight"
            return render_template('bnfts.html', tbl=tbl)


@fantasticsearch.route('/<path:filename>', methods=['GET', 'POST'])
def download(filename):
    return send_from_directory(directory=fantasticsearch.static_folder, filename='PS/' + filename.strip('/home/')) # Get rid of this for production using NGINX

@fantasticsearch.route('/RS_help.pdf', methods=['GET', 'POST'])
def help_docs():
    return send_from_directory(directory=fantasticsearch.static_folder, filename='RS_help.pdf') 

@fantasticsearch.route('/', methods=['GET', 'POST'])
def mem_select():
    if request.method == 'POST':
        fst = request.form['first'].upper().strip()
        lst = request.form['last'].upper().strip()
        dob = request.form['dob'].strip()
        phone = request.form['phone'].strip()
        pcp = request.form['pcp'].strip()
        memid = request.form['memid'].strip()
        resp = make_response(redirect(url_for('mem', fst=fst, lst=lst, dob=dob, phone=phone, pcp=pcp, memid=memid)))
        resp.set_cookie('fst', value=fst)
        resp.set_cookie('lst', value=lst)
        resp.set_cookie('dob', value=dob)
        resp.set_cookie('phone', value=phone)
        resp.set_cookie('pcp', value=pcp)
        resp.set_cookie('memid', value=memid)
        return resp
    return render_template('member.html')

@fantasticsearch.route('/select', methods=['GET', 'POST'])
def select(mpi):
    rsp = make_response(url_for('search', mpi=mpi))
    rsp.set_cookie('mpi', mpi)
    return rsp

@fantasticsearch.route('/notes', methods=['GET', 'POST'])
def notes():
    if request.method == 'POST':
        body_d = {}
        mpi = request.cookies.get('mpi').encode('utf-8')
        nts = request.form['nts']
        if len(nts) > 0:
            body_d["txt1"] = nts
            body_d["mpi"] = mpi
            body_d["db"] = "ScoutNote"
            body_d["date"] = datetime.now().isoformat().split('T')[0]
            body_d["year"] = datetime.now().year
            es.index(index='nts', doc_type="supp", body=body_d)
        resp = make_response(redirect(url_for('search')))
        return resp
    return render_template('scout_note.html')

def dt_fmt(ss):
    ss = ss.replace(' ', '').replace('-','/')
    try:
        return datetime.strptime(ss, '%m/%d/%Y')
    except ValueError:
        pass
    try:
        return datetime.strptime(ss, '%Y/%m/%d')
    except ValueError:
        pass


@fantasticsearch.route('/mem', methods=['GET', 'POST'])
def mem(index=mm_idx):
    fst = request.cookies.get('fst').encode('utf-8').strip()
    lst = request.cookies.get('lst').encode('utf-8').strip()
    dob = request.cookies.get('dob').encode('utf-8')
    phone = request.cookies.get('phone').encode('utf-8').replace('-', '').replace('(', '').replace(')', '')
    pcp = request.cookies.get('pcp').encode('utf-8').strip()
    memid = request.cookies.get('memid').encode('utf-8').strip()
    if dob == '':
        qq = {"query": {"bool": {"should": [{"match": {"first": fst}}, {"match": {"last": lst}},{"match_phrase": {"phone": phone}},{"match": {"pcp": pcp}}, {"match_phrase": {"memid": memid}}], "must_not": [{"match_phrase": {"hp": "HERITAGE COMMERCIAL EMPLOYEE"}}, {"match_phrase": {"hp": "HERITAGE PROVIDER NETWORK"}}]}}, "size": 5}
    else:
        dob = dt_fmt(dob)
        qq = {"query": {"bool": {"should": [{"match_phrase": {"birth": dob}}, {"match": {"first": fst}}, {"match": {"last": lst}},{"match_phrase": {"phone": phone}},{"match": {"pcp": pcp}},{"match_phrase": {"memid": memid}}], "must_not": [{"match_phrase": {"hp": "HERITAGE COMMERCIAL EMPLOYEE"}}, {"match_phrase": {"hp": "HERITAGE PROVIDER NETWORK"}}]}}, "size": 3}
    res = es.search(index=mm_idx, doc_type="supp", body=qq)
    if request.method == 'POST':
        mpi = request.form['mpi']
        rsp = make_response(redirect(url_for('search', mpi=mpi)))
        rsp.set_cookie('mpi', mpi)
        res = es.search(index=mm_idx, doc_type="supp", body={"query": {"match": {"mpi": mpi}}})
        if res['hits']['hits'][0]['_source']['add1'] is not None:
            add1s = res['hits']['hits'][0]['_source']['add1']
            rsp.set_cookie('add1s', add1s)
        if res['hits']['hits'][0]['_source']['add2'] is not None:
            add2s = res['hits']['hits'][0]['_source']['add2']
            rsp.set_cookie('add2s', add2s)
        if res['hits']['hits'][0]['_source']['birth'] is not None:
            dobs = res['hits']['hits'][0]['_source']['birth']
            rsp.set_cookie('dobs', dobs)
        if res['hits']['hits'][0]['_source']['first'] is not None:
            firsts = res['hits']['hits'][0]['_source']['first']
            rsp.set_cookie('firsts', firsts)
        if res['hits']['hits'][0]['_source']['last'] is not None:
            lasts = res['hits']['hits'][0]['_source']['last']
            rsp.set_cookie('lasts', lasts)
        if res['hits']['hits'][0]['_source']['hp'] is not None:
            hps = res['hits']['hits'][0]['_source']['hp']
            rsp.set_cookie('hps', hps)
        if res['hits']['hits'][0]['_source']['pcp'] is not None:
            pcps = res['hits']['hits'][0]['_source']['pcp']
            rsp.set_cookie('pcps', pcps)
        if res['hits']['hits'][0]['_source']['phone'] is not None:
            phones = res['hits']['hits'][0]['_source']['phone']
            rsp.set_cookie('phones', phones)
        if res['hits']['hits'][0]['_source']['sex'] is not None:
            sexs = res['hits']['hits'][0]['_source']['sex']
            rsp.set_cookie('sexs', sexs)
        if res['hits']['hits'][0]['_source']['memid'] is not None:
            memids = res['hits']['hits'][0]['_source']['memid']
            rsp.set_cookie('memids', memids)
        if res['hits']['hits'][0]['_source']['opt'] is not None:
            opt = res['hits']['hits'][0]['_source']['opt']
            rsp.set_cookie('opt', opt)
        if res['hits']['hits'][0]['_source']['hpcode'] is not None:
            hpcode = res['hits']['hits'][0]['_source']['hpcode']
            rsp.set_cookie('hpcode', hpcode)
        if res['hits']['hits'][0]['_source']['company'] is not None:
            company = res['hits']['hits'][0]['_source']['company']
            rsp.set_cookie('company', company)
        return rsp
    return render_template('mem_sel.html', res=res)


def rndr(res):
    tz_lst = []
    for kk in res['hits']['hits']:
        if 'date' in kk['_source']:
            tz_lst.append(kk['_source']['date'])
    tz = []
    for k in Counter(tz_lst):
        body_d = dict()
        body_d['date'] = str(k)[:7]
        body_d['count'] = str(int(Counter(tz_lst)[k]))
        tz.append(body_d)
    return tz

@fantasticsearch.route('/search')
def search():
    c_time = datetime.utcnow()
    c_time = c_time.strftime('%m/%d/%Y')
    mpi = request.cookies.get('mpi').encode('utf-8')
    add1s = request.cookies.get('add1s').encode('utf-8')
    add2s = request.cookies.get('add2s').encode('utf-8')
    dobs = request.cookies.get('dobs').encode('utf-8')
    sexs = request.cookies.get('sexs').encode('utf-8')
    firsts = request.cookies.get('firsts').encode('utf-8')
    lasts = request.cookies.get('lasts').encode('utf-8')
    hps = request.cookies.get('hps').encode('utf-8')
    pcps = request.cookies.get('pcps').encode('utf-8')
    phones = request.cookies.get('phones').encode('utf-8')
    memids = request.cookies.get('memids').encode('utf-8')
    opt = request.cookies.get('opt').encode('utf-8')
    hpcode = request.cookies.get('hpcode').encode('utf-8')
    company = request.cookies.get('company').encode('utf-8')
    term = request.args.get('term', '').strip()
    filters = request.args.get('filter', '')
    page = request.args.get('page', '')
    if not term or term == "null":
        term = "*"
    results = performQuery(term, filters, page)
    tz = rndr(results)
    return render_template('index.html', results=results, filters=filters, term=term, page=page, mpi=mpi, memids=memids, opt=opt, firsts=firsts, lasts=lasts, dobs=dobs, sexs=sexs, hps=hps, pcps=pcps, phones=phones, add1s=add1s, add2s=add2s, hpcode=hpcode, company=company, tz=tz, c_time=c_time)


def performQuery(term, filterString, page):
    filters = parseFilters(filterString)
    term = term.encode('utf-8')
    query = getBasicQuery(filters, term, page)
    result = es.search(index=indexName, body=query)
    return result 


def parseFilters(filters):
	filterDict = {}
	for f in filters.split(','):
		if f and len(f.split('-')) == 2: 	
			typ = f.split('-')[0].encode('utf-8')
			val = f.split('-')[1].encode('utf-8')
			filterDict[typ] = val
	return filterDict
@fantasticsearch.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500

def q_help(tt):
    if '~' not in tt:
        lst = es.search(index="syn", doc_type="supp", body={"query": {"match": {"text": tt}}})
        if len(lst['hits']['hits']) > 0:
            return lst['hits']['hits'][0]['_source']['tags']
        else:
            return [tt + '~']
    else:
        return [tt]

def q_exp(tm):
    if '"' not in tm:
        x_lst = []
        for tt in tm.split(' '):
            x_lst += q_help(tt)
        x_lst = [str(x) for x in x_lst]
        x_lst = list(set(x_lst))
        N = len(x_lst)
        x_lst = [x + '^' + str(N-idx) for idx, x in enumerate(x_lst)]
        qs = ' OR '.join(x_lst)
        if 'AND' in tm:
            qs = tm
    else:
        qs = tm
    return qs

#Those queries implement the filters as AND filter and the query as query_string_query
def getBasicQuery(filters, term, page):
    query = None
    try:
        mpi = request.cookies.get('mpi').encode('utf-8')
    except:
        mpi = '909cfb6d-3412-48b8-89a5-c40bd08addc5'
    mustClauses = []
    #mustClauses = [{"term": {"mpi": mpi}}]
    if not page:
        page = 0
        start = int(page) * 20
    else:
        start = (int(page) - 1)* 20
    for k,v in filters.iteritems():
        clause = {"term" : { k : v }}
        mustClauses.append(clause)
    shdClauses = []
    mpi_lst = [mpi, '123']
    for ll in mpi_lst:
	shdClauses.append({"term": {"mpi": ll}})
    fltClauses = {"bool" : {"should": shdClauses}}
    mustClauses.append(fltClauses)
    filterClauses = {"and" : mustClauses }
    aggregations = {}
    for el in aggregationFields:
        if el == 'spclt':
            aggregations[el] = {"terms": {"field": el, "size": 0, "order": {"_term": "asc"}}}
        elif el == 'year':
            aggregations[el] = {"terms": {"field": el, "size": 0, "order": {"_term": "desc"}}}
        elif el == 'tags':
            aggregations[el] = {"terms" : {"field" : el, "size": 100 }}
        else:
            aggregations[el] = {"terms" : {"field" : el, "size": 0 }}
    #aggregations["date"] = {"date_histogram": {"field": "date", "interval": "year"}}
    query = {"query": {"filtered": {"filter": filterClauses, "query": {"query_string" : {"fields": ["text", "nm", "txt1", "txt2", "txt3", "txt4", "txt5", "txt6", "txt7"], "query" : q_exp(term)} } } }, "size": 20, "from": start, "aggs" : aggregations, "sort": {"date": {"order": "desc"}}}
    return query
