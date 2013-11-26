from collections import OrderedDict
import logging
import os
import shutil
import re
import xmlwitch
from start import Distributer

__author__ = 'yijiliu'

##############################
# This file contains all basic classes for each modeling project.
# # Project: Class contains settings for project level. A project is made of multiple stages.
# # Stage: Class contains info for stage level. A stage is made of multiple parallel tasks.
# # Task: Class contains settings for single task. Each task should be external program.
#
# Date: 2013-Oct-25
##############################


class DataMeta:
    def __init__(self):
        self.id = []
        self.bad = []
        self.weight = []
        self.report_vars = []
        self.model_vars = []


class DataFile:
    def __init__(self, fconfig, is_folder=0, is_package=0):
        self.is_folder = True if is_folder == 1 else False
        self.is_package = True if is_package == 1 else False
        if type(fconfig) in (str, unicode) :
            self.is_data = False
            self.is_output = not self.is_package
            self.id = 'na'
            self.path = fconfig
        else :
            if '@type' in fconfig :
                self.is_output = True if fconfig['@type'] == 'output' else False
                self.is_data = True if fconfig['@type'] == 'data' else False
            else :
                self.is_output = not self.is_package   # TODO: ERROR! default value of the input files should be different
                self.is_data = False
            self.id = fconfig['@id'] if '@id' in fconfig else 'na'
            self.path = fconfig['#text']


class Task:
    def __init__(self, tconfig):
        self.name = tconfig['name']
        self.jobtype = tconfig['job_type'].lower()
        self.script = tconfig['script']
        self.package = tconfig['package']
        skip = tconfig.get('skip_task', 'no')
        self.skip = skip is not None and skip.lower() == 'yes'
        self.input = []
        if 'task_input' in tconfig and tconfig['task_input'] is not None and 'file' in tconfig['task_input']:
            if type(tconfig['task_input']['file']) is not list :
                self.input.append(DataFile(tconfig['task_input']['file']))
            else :
                for item in tconfig['task_input']['file'] :
                    self.input.append(DataFile(item))
        self.output = None
        if tconfig['task_output'] is not None :
            self.output = []
            if 'file' in tconfig['task_output'] :
                if type(tconfig['task_output']['file']) is not list :
                    self.output.append(DataFile(tconfig['task_output']['file']))
                else:
                    for item in tconfig['task_output']['file'] :
                        self.output.append(DataFile(item))
            if 'folder' in tconfig['task_output'] :
                if type(tconfig['task_output']['folder']) is not list :
                    self.output.append(DataFile(tconfig['task_output']['folder'], 1))
                else:
                    for item in tconfig['task_output']['folder'] :
                        self.output.append(DataFile(item, 1))
        self.complete = False
        self.delievered = False


class Stage:
    def __init__(self, sconfig):
        self.name = sconfig['name']
        self.tasks = {}
        self.script = sconfig['stage_script']
        self.package = sconfig['stage_package']
        skip = sconfig.get('skip_stage', 'no')
        self.skip = skip is not None and skip.lower() == 'yes'
        self.triggered = False if self.script is not None else True
        self.input = []
        if 'stage_input' in sconfig and sconfig['stage_input'] is not None and 'file' in sconfig['stage_input']:
            if type(sconfig['stage_input']['file']) is not list :
                self.input.append(DataFile(sconfig['stage_input']['file']))
            else :
                for item in sconfig['stage_input']['file'] :
                    self.input.append(DataFile(item))
        self.output = None
        if sconfig['stage_output'] is not None :
            self.output = []
            if 'file' in sconfig['stage_output'] :
                if type(sconfig['stage_output']['file']) is not list :
                    self.output.append(DataFile(sconfig['stage_output']['file']))
                else :
                    for item in sconfig['stage_output']['file'] :
                        self.output.append(DataFile(item))
            if 'folder' in sconfig['stage_output'] :
                if type(sconfig['stage_output']['folder']) is not list :
                    self.output.append(DataFile(sconfig['stage_output']['folder'], 1))
                else :
                    for item in sconfig['stage_output']['folder'] :
                        self.output.append(DataFile(item, 1))
        if type(sconfig['tasks']['task']) is not list :
            task = Task(sconfig['tasks']['task'])
            self.tasks[task.name] = task
        else :
            for item in sconfig['tasks']['task'] :
                task = Task(item)
                self.tasks[task.name] = task

    def checkDeliever(self):
        for task in self.tasks.itervalues() :
            if task.delievered :
                return True
        return False

    def checkComplete(self):
        for task in self.tasks.itervalues() :
            if not task.complete:
                return False
        return True


class Project:
    def __init__(self, pconfig):
        self.stages = OrderedDict()
        self.name = pconfig['name']
        self.inner_name = ""
        self.email = pconfig['email'] # TODO: this can be changed to multiple receivers
        self.datameta = DataMeta()
        self.datameta.id.extend(pconfig['record_ids']['id'])
        self.datameta.bad.append(pconfig['bad_indicator'])
        self.datameta.weight.append(pconfig['weight_indicator'])
        self.clean = pconfig.get('clean_after_success', 'no').lower() == 'yes'
        if 'model_var_list' in pconfig and os.path.exists(pconfig['model_var_list']) :
            for line in open(pconfig['model_var_list']) :
                self.datameta.model_vars.append(line.strip())
        if 'report_var_list' in pconfig and os.path.exists(pconfig['report_var_list']) :
            for line in open(pconfig['report_var_list']) :
                self.datameta.report_vars.append(line.strip())
        for item in pconfig['stages']['stage'] :
            stage = Stage(item)
            self.stages[stage.name] = stage
        self.datafiles = []
        if type(pconfig['project_input']['file']) is not list:
            self.datafiles.append(DataFile(pconfig['project_input']['file']))
        else:
            for item in pconfig['project_input']['file']:
                self.datafiles.append(DataFile(item))
        for stage_name, stage in self.stages.iteritems() :
            if stage.input is not None :
                self.datafiles.extend(stage.input)
            if stage.output is not None :
                self.datafiles.extend(stage.output)
            for task_name, task in stage.tasks.iteritems() :
                if task.input is not None :
                    self.datafiles.extend(task.input)
                if task.output is not None :
                    self.datafiles.extend(task.output)
        #TODO: need to create the input file list for each worktask based on datafile id and stuffs
        for stage_name, stage in self.stages.iteritems() :
            for task_name, task in stage.tasks.iteritems() :
                if task.script is None or task.script == '' :
                    continue
                for file_id in re.findall(r'%\w+', task.script) :
                    files = [x for x in self.datafiles if x.id == file_id[1:]]
                    if len(files) == 1 :
                        task.input.append(files[0])
                    else :
                        logging.error("file replacer error : {0}".format(repr(files)))
        self.deactivate = False

    def nextStage(self, system):
        if self.deactivate :
            return -1
        stage = self.findUndelieveredStage()
        if stage is None :
            return -1
        for taskname, task in stage.tasks.iteritems() :
            if task.skip :
                continue
            target_engine, same_engine = system.findEngineByType(task.jobtype)
            target_path = "" if same_engine else target_engine.address + ":"
            target_path += target_engine.taskpool
            taskcontent = self.createWorkTask(task)
            distributor = Distributer(target_path, os.sep.join([system.engine.delivery, self.inner_name]), taskname, taskcontent, same_engine)
            distributor.setDaemon(True)
            distributor.start()
            task.delievered = True
        if stage.checkComplete() and not stage.skip and not stage.triggered:
            # TODO: start stage script
            print "stage started!"
            stage.triggered = True

    def findUndelieveredStage(self):
        for stagename, stage in self.stages.iteritems() :
            if not stage.checkDeliever() :
                return stage
        return None

    def createWorkTask(self, task):
        work_task_xml = xmlwitch.Builder()
        with work_task_xml.worktask() :
            work_task_xml.project(self.name)
            work_task_xml.project_id(self.inner_name)
            work_task_xml.task_name(task.name)
            work_task_xml.job_type(task.jobtype)
            work_task_xml.script(task.script)
            work_task_xml.package(task.package)
            with work_task_xml.input():
                for item in task.input:
                    if item.is_folder:
                        work_task_xml.folder(item.path, id=item.id)
                    else :
                        work_task_xml.file(item.path, id=item.id)
            with work_task_xml.output():
                for item in task.output:
                    if item.is_output:
                        datatype = 'output'
                    elif item.is_data:
                        datatype = 'data'
                    else :
                        datatype = 'na'
                    if item.is_folder:
                        work_task_xml.folder(item.path, id=item.id, type=datatype)
                    else :
                        work_task_xml.file(item.path, id=item.id, type=datatype)
        return str(work_task_xml)



class WorkTask:
    def __init__(self, wconfig):
        self.project_name = wconfig['project']
        self.project_id = wconfig['project_id']
        self.task_name = wconfig['task_name']
        self.jobtype = wconfig['job_type']
        self.script = wconfig['script']
        self.package = wconfig['package']
        self.inputs = []
        if type(wconfig['input']['file']) == list :
            for item in wconfig['input']['file'] :
                self.inputs.append(DataFile(item))
        else :
            self.inputs.append(DataFile(wconfig['input']['file']))
        self.outputs = []
        if type(wconfig['output']['file']) == list :
            for item in wconfig['output']['file'] :
                self.outputs.append(DataFile(item))
        else :
            self.outputs.append(DataFile(wconfig['output']['file']))
        if type(wconfig['output']['folder']) == list :
            for item in wconfig['output']['folder'] :
                self.outputs.append(DataFile(item))
        else :
            self.outputs.append(DataFile(wconfig['output']['folder'], 1))

    def check(self, system):
        missing_files = []
        # TODO: check where to check for the input files. The output folder or the temp folder or both?
        for input_file in self.inputs :
            path = input_file.path if input_file.path.startswith(os.sep) or input_file.path[1] == ":" \
                                   else os.sep.join((system.engine.temp, self.project_id, input_file.path))
            if not os.path.exists(path):
                # Create a list of files needs to be copied from controller to local
                missing_files.append(input_file)
            else :
                input_file.path = path
        if self.package is not None and self.package.strip() != "" :
            package_path = self.package if self.package.startswith(os.sep) or self.package[1] == ":" \
                                        else os.sep.join((system.engine.temp, self.project_id, self.package))
            if not os.path.exists(package_path) :
                missing_files.append(DataFile(self.package, is_package=1))
        return missing_files


class Message:
    def __init__(self, mconfig):
        pass


class Command:
    def __init__(self, cconfig, system):
        self.project_name = cconfig.get('project', None)
        self.email = cconfig.get('email', None)
        self.command = cconfig['command']
        self.projects = {}
        if self.project_name is not None and self.email is not None :
            candidates = system.findProjectByName(self.project_name, self.email)
            for candidate in candidates:
                self.projects[candidate.inner_name] = candidate



