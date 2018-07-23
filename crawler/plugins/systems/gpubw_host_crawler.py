import logging
import ctypes
import platform
import os
import sys
import subprocess
import psutil
import json
from icrawl_plugin import IHostCrawler

logger = logging.getLogger('crawlutils')


class GPUHostCrawler(IHostCrawler):

    def get_feature(self):
        return 'gpubw'

    def create_feature(self, emit_location):
        files = os.listdir(emit_location)
        files.sort()
        features = []
        for f in files:
            filepath = emit_location + "/" + f
            if not f.endswith('.tmp'):
                features = features + open(filepath).readlines()
                os.remove(filepath)
        for feature in features:
            feature_json = json.loads(feature)
            key = feature_json['timestamp']
            val = feature_json
            type = 'gpubw'
            yield (key, val, type) 

    def start_collector(self, emit_duration, emit_location, kill_collector):
	#if 'nvlmon_ppc64le_p9' in [p.name() for p in psutil.process_iter()]:
	procs = [p for p in psutil.process_iter() if 'nvlmon_ppc64le_p9' in p.name()] 
        if procs != []:    
            if kill_collector == 'True':
                for p in procs:
                    p.kill()                 
            return
        collector_cmd = "python " + os.getcwd() + "/utils/nvlmon.py " + emit_duration + " " + emit_location
        try:
            subprocess.Popen(['sh', '-c', collector_cmd])
        except OSerror as err:
            print "Failed to init nvlmon: " + err

    def validate_location(self, emit_location):
        if not os.path.exists(emit_location):
            os.makedirs(emit_location)
            os.chown(emit_location, 1000,1000)

    def check_platform(self):
        platform_ok = False
        if 'ppc' in platform.machine() and platform.architecture()[0] == '64bit':
            platform_ok = True
        return platform_ok

    def crawl(self, **kwargs):
        import pdb
        pdb.set_trace()
        logger.debug('Crawling NVLink bandwidth on the host')
        emit_duration = kwargs.get('emit_duration','30')
        emit_location = kwargs.get('emit_location',"/tmp/gpu_bw_stats")
        kill_collector = kwargs.get('kill_collector',"False")
        self.validate_location(emit_location)
        if not self.check_platform():
            return
        self.start_collector(emit_duration, emit_location, kill_collector)
        return self.create_feature(emit_location) 

