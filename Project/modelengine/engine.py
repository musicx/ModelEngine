from collections import defaultdict
import logging
from os import path
import os
import random
import xmltodict

__author__ = 'yijiliu'

##############################
# This file contains the engine and system classes.
# # System: the system wise information. It contains multiple engines.
# # Engine: the information of each engine.
#
# Date: 2013-Oct-25
##############################


class System:
    def __init__(self, config_path):
        if not path.exists(config_path):
            logging.error("Please make sure the system configuration file exists in proper place!")
            raise IOError
        lines = open(config_path).read()
        try:
            doc = xmltodict.parse(lines)
        except Exception:
            logging.error("Please make sure the system configuration file has a proper format!")
            raise IOError
        self.interval = int(float(doc['configuration']['check_interval']))
        self.engines = {}
        self.username = doc['configuration']['username']
        self.num_worker = int(float(doc['configuration']['worker_number']))
        self.num_copier = int(float(doc['configuration']['copier_number']))
        for server in doc['configuration']['engines']['server']:
            engine = Engine(server, self.username)
            self.engines[engine.name] = engine
            if engine.name == doc['configuration']['engine_name']:
                self.engine = engine
            if engine.controller :
                self.controller = engine
        self.engine.check()
        self.projects = {}
        self.engine.worktasks = defaultdict(list)


    def findEngineByType(self, jobtype):
        candidates = []
        for enginename, engine in self.engines.iteritems() :
            if jobtype.lower() in engine.types :
                candidates.append(engine)
        ind = random.randint(0, len(candidates) - 1)
        return candidates[ind], candidates[ind].name == self.engine.name


    def findProjectByName(self, project_name, email):
        candidates = []
        for inner_name, project in self.projects.iteritems() :
            if project_name == project.name and email == project.email :
                candidates.append(project)
        return candidates

class Engine:
    def __init__(self, engine_config, username):
        self.name = engine_config['name']
        self.controller = engine_config.get('controller', 'no') == 'yes'
        self.address = engine_config['address']
        self.taskpool = engine_config['taskpool']
        self.delivery = engine_config['delivery']
        self.temp = engine_config['temp']
        self.output = engine_config['output']
        if type(engine_config['job_types']['type']) in (str, unicode) :
            self.types = set(engine_config['job_types']['type'].lower())
        else :
            self.types = set([x.lower() for x in engine_config['job_types']['type']])
        self.username = username
        self.rsa_key = engine_config['rsa_key']
        self.worktasks = {}

    def check(self):
        if not os.path.exists(self.temp) :
            os.makedirs(self.temp)
        if not os.path.exists(self.delivery) :
            os.makedirs(self.delivery)
        if not os.path.exists(self.output) :
            os.makedirs(self.output)
        if not os.path.exists(self.taskpool) :
            os.makedirs(self.taskpool)
