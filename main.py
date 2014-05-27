from flask import Flask, render_template, Response, url_for
import subprocess
import sys
import time
import urllib2
from jinja2 import Environment
from jinja2.loaders import FileSystemLoader

app = Flask(__name__)

@app.route('/')
def index(name=None):
    return render_template('index.html', name=name)

@app.route('/storm')
def storm():
    url = "http://ec2-54-200-149-130.us-west-2.compute.amazonaws.com:8000/log?file=worker-6700.log"
    def inner():
        while True:
            contents = urllib2.urlopen(url).readlines()[-5:]
            for line in contents:
                if 'Processing received' in line:
                    yield '%s<br/>\n' % line
            time.sleep(1)
    env = Environment(loader=FileSystemLoader('templates'))
    tmpl = env.get_template('storm.html')
    return Response(tmpl.generate(result=inner()))

@app.route('/hivemr')
def hivemr():
    def inner():
        yield 'Starting query, please wait...<br/>\n'
        cmd = "hive -f /tmp/git/hive-json/tweets_query.hsql".split()
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(process.stdout.readline, ''):
            yield '%s<br/>\n' % line
        time.sleep(1) 
    env = Environment(loader=FileSystemLoader('templates'))
    tmpl = env.get_template('twitterhivemr.html')
    return Response(tmpl.generate(result=inner()))

@app.route('/hivetez')
def hivetez():
    def inner():
        yield 'Starting query, please wait...<br/>\n'
        cmd = "hive -f /tmp/git/hive-json/tweets_query.hsql -hiveconf hive.execution.engine=tez".split()
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(process.stdout.readline, ''):
            yield '%s<br/>\n' % line
        time.sleep(1)
    env = Environment(loader=FileSystemLoader('templates'))
    tmpl = env.get_template('twitterhivetez.html')
    return Response(tmpl.generate(result=inner()))

app.run(host='0.0.0.0', debug=True)
