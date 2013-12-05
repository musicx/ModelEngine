from collections import OrderedDict
import logging
import os
import shutil
import re
import time
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
    def __init__(self, fconfig, is_input=False, is_output=False, is_folder=False, is_package=False):
        self.is_folder = is_folder
        self.is_package = is_package
        self.is_output = is_output
        self.is_input = is_input
        if type(fconfig) in (str, unicode) :
            self.id = '##'
            self.path = fconfig
        else :
            if '@type' in fconfig :
                self.is_output = True if fconfig['@type'] == 'output' else False
                self.is_input = True if fconfig['@type'] == 'data' else False
            self.id = fconfig['@id'] if '@id' in fconfig else '##'
            self.path = fconfig['#text']
        self.relative_path = self.path if self.is_folder else ""

class Task:
    def __init__(self, tconfig):
        self.name = "tsk_" + tconfig['name']
        self.jobtype = tconfig['job_type'].lower()
        self.script = tconfig['script']
        self.package = tconfig['package']
        skip = tconfig.get('skip_task', 'no')
        self.skip = skip is not None and skip.lower() == 'yes'
        self.input = []
        if 'task_input' in tconfig and tconfig['task_input'] is not None and 'file' in tconfig['task_input']:
            if type(tconfig['task_input']['file']) is not list :
                self.input.append(DataFile(tconfig['task_input']['file'], is_input=True))
            else :
                for item in tconfig['task_input']['file'] :
                    self.input.append(DataFile(item, is_input=True))
        self.output = None
        if tconfig['task_output'] is not None :
            self.output = []
            if 'file' in tconfig['task_output'] :
                if type(tconfig['task_output']['file']) is not list :
                    self.output.append(DataFile(tconfig['task_output']['file'], is_output=True))
                else:
                    for item in tconfig['task_output']['file'] :
                        self.output.append(DataFile(item, is_output=True))
            if 'folder' in tconfig['task_output'] :
                if type(tconfig['task_output']['folder']) is not list :
                    self.output.append(DataFile(tconfig['task_output']['folder'], is_folder=True, is_output=True))
                else:
                    for item in tconfig['task_output']['folder'] :
                        self.output.append(DataFile(item, is_folder=True, is_output=True))
        self.complete = False
        self.delievered = False


class Stage:
    def __init__(self, sconfig):
        self.name = "stg_" + sconfig['name']
        self.tasks = {}
        self.script = sconfig['stage_script']
        self.package = sconfig['stage_package']
        skip = sconfig.get('skip_stage', 'no')
        self.skip = skip is not None and skip.lower() == 'yes'
        self.delievered = False if self.script is not None else True
        self.complete = self.delievered
        self.input = []
        if 'stage_input' in sconfig and sconfig['stage_input'] is not None and 'file' in sconfig['stage_input']:
            if type(sconfig['stage_input']['file']) is not list :
                self.input.append(DataFile(sconfig['stage_input']['file'], is_input=True))
            else :
                for item in sconfig['stage_input']['file'] :
                    self.input.append(DataFile(item, is_input=True))
        self.output = None
        if sconfig['stage_output'] is not None :
            self.output = []
            if 'file' in sconfig['stage_output'] :
                if type(sconfig['stage_output']['file']) is not list :
                    self.output.append(DataFile(sconfig['stage_output']['file'], is_output=True))
                else :
                    for item in sconfig['stage_output']['file'] :
                        self.output.append(DataFile(item, is_output=True))
            if 'folder' in sconfig['stage_output'] :
                if type(sconfig['stage_output']['folder']) is not list :
                    self.output.append(DataFile(sconfig['stage_output']['folder'], is_folder=True, is_output=True))
                else :
                    for item in sconfig['stage_output']['folder'] :
                        self.output.append(DataFile(item, is_folder=True, is_output=True))
        if type(sconfig['tasks']['task']) is not list :
            task = Task(sconfig['tasks']['task'])
            self.tasks[task.name] = task
        else :
            for item in sconfig['tasks']['task'] :
                task = Task(item)
                self.tasks[task.name] = task

    def allTaskDelievered(self):
        for task in self.tasks.itervalues() :
            if task.delievered :
                return True
        return False

    def allTaskComplete(self):
        for task in self.tasks.itervalues() :
            if not task.complete:
                return False
        return True


class Project:
    def __init__(self, pconfig, system):
        self.system = system
        self.stages = OrderedDict()
        self.name = pconfig['name']
        self.project_id = "{0}_{1}".format(self.name, long(time.time()))
        while self.project_id in self.system.projects:
            self.project_id = "{0}_{1}".format(self.name, long(time.time()))
        self.email = pconfig['email'] # TODO: this can be changed to multiple receivers
        self.clean = pconfig.get('clean_after_success', None) is not None \
                     and pconfig['clean_after_success'].lower() == 'yes'

        #self.datameta = DataMeta()
        #self.datameta.id.extend(pconfig['record_ids']['id'])
        #self.datameta.bad.append(pconfig['bad_indicator'])
        #self.datameta.weight.append(pconfig['weight_indicator'])
        #if 'model_var_list' in pconfig and os.path.exists(pconfig['model_var_list']) :
        #    for line in open(pconfig['model_var_list']) :
        #        self.datameta.model_vars.append(line.strip())
        #if 'report_var_list' in pconfig and os.path.exists(pconfig['report_var_list']) :
        #    for line in open(pconfig['report_var_list']) :
        #        self.datameta.report_vars.append(line.strip())

        for item in pconfig['stages']['stage'] :
            stage = Stage(item)
            self.stages[stage.name] = stage
        self.datafiles = []
        if 'file' in pconfig['project_input'] and type(pconfig['project_input']['file']) is not list:
            self.datafiles.append(DataFile(pconfig['project_input']['file']))
        elif 'file' in pconfig['project_input']:
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
        # check the uniqueness of each file id!
        ids = set()
        for item in self.datafiles :
            if item.id == "##":
                continue
            if item.id in ids :
                logging.error("ID conflict detected in the configuration" + item.id)
                raise ValueError
            else :
                ids.add(item.id)
        #create the input file list for each worktask based on datafile id and stuffs
        for stage_name, stage in self.stages.iteritems() :
            for task_name, task in stage.tasks.iteritems() :
                if task.script is None or task.script == '' :
                    continue
                for file_id in re.findall(r'%\w+', task.script) :
                    files = [x for x in self.datafiles if x.id == file_id[1:]]
                    if len(files) == 1 :
                        if any([x.id == files[0].id for x in task.input]) :
                            continue
                        if any([x.id == files[0].id for x in task.output]) :
                            continue
                        task.input.append(files[0])
                    else :
                        logging.error("file replacer error : {0}".format(repr(files)))
            for file_id in re.findall(r'%\w+', stage.script):
                files = [x for x in self.datafiles if x.id == file_id[1:]]
                if len(files) == 1:
                    if any([x.id == files[0].id for x in stage.input]) :
                        continue
                    if any([x.id == files[0].id for x in stage.output]) :
                        continue
                    stage.input.append(files[0])
                else:
                    logging.error("file replacer error : {0}".format(repr(files)))
            #TODO: consider update the is_input / is_output attribute for each DataFile depending on where they are read
        self.deactivate = False

    def nextTasks(self):
        if self.deactivate :
            return -1
        stage = self.findUndelieveredStage()
        if stage is None :
            return 0
        for taskname, task in stage.tasks.iteritems() :
            if task.skip or task.delievered:
                continue
            target_engine, same_engine = self.system.findEngineByType(task.jobtype)
            target_path = "" if same_engine else target_engine.username + "@" + target_engine.address + ":"
            target_path += target_engine.taskpool
            taskcontent = self.createWorkTask(task, self.system.engine)
            distributor = Distributer(self.system.engine, os.sep.join([self.system.engine.delivery, self.project_id]),
                                      taskname, taskcontent, target_path, same_engine)
            #distributor.setDaemon(True)
            distributor.start()
            task.delievered = True
        if stage.allTaskComplete() and not stage.skip and not stage.delievered:
            script_task_content = self.createWorkTask(stage, self.system.engine)
            distributor = Distributer(self.system.engine, os.sep.join([self.system.engine.delivery, self.project_id]),
                                      stage.name, script_task_content, self.system.engine.taskpool, True)
            distributor.start()
            stage.delievered = True
        return 1

    def findUndelieveredStage(self):
        for stagename, stage in self.stages.iteritems() :
            if not (stage.allTaskDelievered() and stage.delievered) :
                return stage
        return None

    def findIncompleteStage(self):
        for stagename, stage in self.stages.iteritems() :
            if not (stage.allTaskComplete() and stage.complete) :
                return stage
        return None

    def createWorkTask(self, task, engine):
        work_task_xml = xmlwitch.Builder()
        with work_task_xml.worktask() :
            #work_task_xml.project(self.name)
            work_task_xml.project_id(self.project_id)
            work_task_xml.task_name(task.name)
            work_task_xml.script(task.script)
            if task.package is not None:
                package = self.findAbsolutePath(task.package, engine)
                if not package:
                    # TODO: handle error
                    pass
                work_task_xml.package(task.package)
            else :
                work_task_xml.package()
            with work_task_xml.input():
                for item in task.input:
                    path = self.findAbsolutePath(item.path, engine)
                    if not path :
                        # TODO : handle error
                        pass
                    if item.is_folder:
                        work_task_xml.folder(path, id=item.id)
                    else :
                        work_task_xml.file(path, id=item.id)
            with work_task_xml.output():
                for item in task.output:
                    if item.is_output:
                        datatype = 'output'
                    elif item.is_input:
                        datatype = 'data'
                    else :
                        datatype = 'na'
                    if item.is_folder:
                        work_task_xml.folder(item.path, id=item.id, type=datatype)
                    else :
                        work_task_xml.file(item.path, id=item.id, type=datatype)
        return str(work_task_xml)

    def findAbsolutePath(self, path, engine):
        if (path.startswith(os.sep) or path[1] == ":") and os.path.exists(path) :
            return path
        elif os.path.exists(os.sep.join([engine.temp, self.project_id, path])) :
            return os.sep.join([engine.temp, self.project_id, path])
        elif os.path.exists(os.sep.join([engine.output, self.project_id, path])) :
            return os.sep.join([engine.output, self.project_id, path])
        else :
            logging.error("File cannot be found : {0}".format(path))
            return None

    def updateStatus(self, taskname, status) :
        stage = self.findIncompleteStage()
        if taskname in stage.tasks :
            if status == 0 :
                stage.tasks[taskname].complete = True
            else :
                self.deactivate = True
        elif taskname == stage.name :
            if status == 0 :
                stage.complete = True
            else :
                self.deactivate = True
        else :
            # ERROR log
            logging.error("Cannot find corresponding task to update!")
            self.deactivate = True
        result = self.nextTasks()
        if result == 0 :
            # TODO: clean the whole project and send command to all slaves
            self.clean()

    def clean(self):
        clean_xml = xmlwitch.Builder()
        with clean_xml.command() :
            clean_xml.project_id(self.project_id)
            clean_xml.command("clean")
        for engine in self.system.engines.itervalue() :
            same_engine = engine.address == self.system.engine.address
            target_folder = "{0}@{1}:{2}".format(engine.username, engine.address, engine.taskpool) \
                            if not same_engine else engine.taskpool
            distributor = Distributer(self.system.engine, os.sep.join([self.system.engine.delivery, self.project_id]),
                                      "clean", str(clean_xml), target_folder, same_engine)
            distributor.start()


class WorkTask:
    def __init__(self, wconfig):
        #self.project_name = wconfig['project']
        self.project_id = wconfig['project_id']
        self.task_name = wconfig['task_name']
        self.script = wconfig['script']
        self.package = wconfig['package']
        self.inputs = []
        if 'file' in wconfig['input'] and type(wconfig['input']['file']) == list :
            for item in wconfig['input']['file'] :
                self.inputs.append(DataFile(item, is_input=True))
        elif 'file' in wconfig['input'] :
            self.inputs.append(DataFile(wconfig['input']['file'], is_input=True))
        self.outputs = []
        if 'file' in wconfig['output'] and type(wconfig['output']['file']) == list :
            for item in wconfig['output']['file'] :
                self.outputs.append(DataFile(item, is_output=True))
        elif 'file' in wconfig['output'] :
            self.outputs.append(DataFile(wconfig['output']['file'], is_output=True))
        if 'folder' in wconfig['output'] and type(wconfig['output']['folder']) == list :
            for item in wconfig['output']['folder'] :
                self.outputs.append(DataFile(item, is_output=True, is_folder=True))
        elif 'folder' in wconfig['output'] :
            self.outputs.append(DataFile(wconfig['output']['folder'], is_output=True, is_folder=True))

    def checkMissing(self, engine):
        """
        check the temp && the output folder for the file, as well as the absolute path if given
        """
        missing_files = []
        for input_file in self.inputs :
            missing_flag = False
            temp_path = input_file.path if input_file.path.startswith(os.sep) or input_file.path[1] == ":" \
                                        else os.sep.join([engine.temp, self.project_id, os.path.basename(input_file.path)])
            output_path = input_file.path if input_file.path.startswith(os.sep) or input_file.path[1] == ":" \
                                          else os.sep.join((engine.output, self.project_id, input_file.path))
            if not os.path.exists(output_path):
                if not os.path.exists(temp_path) :
                    missing_files.append(input_file)
                    missing_flag = True
                else :
                    input_file.path = temp_path
            else :
                input_file.path = output_path
            replace_path = os.sep.join([engine.temp, self.project_id, os.path.basename(input_file.path)]) \
                           if missing_flag else input_file.path
            if input_file.id is not None and input_file.id != "##":
                self.script = self.script.replace("%" + input_file.id, replace_path)
        for output_file in self.outputs :
            file_path = output_file.path if output_file.path.startswith(os.sep) or output_file.path[1] == ":" \
                                         else os.sep.join([engine.temp, self.project_id, output_file.path])
            output_file.path = file_path
            if output_file.id is not None and output_file.id != "##" :
                self.script = self.script.replace("%" + output_file.id, file_path)
        package_missing_flag = False
        if self.package is not None and self.package.strip() != "" :
            package_temp_path = self.package if self.package.startswith(os.sep) or self.package[1] == ":" \
                                             else os.sep.join((engine.temp, self.project_id, self.package))
            package_output_path = self.package if self.package.startswith(os.sep) or self.package[1] == ":" \
                                               else os.sep.join((engine.output, self.project_id, self.package))
            if not os.path.exists(package_output_path) :
                if not os.path.exists(package_temp_path) :
                    missing_files.append(DataFile(self.package, is_package=True))
                    package_missing_flag = True
                else :
                    self.package = package_temp_path
            else :
                self.package = package_output_path
        # replace the package path in the script
        script_part = self.script.split(" ")[1]
        candidate_script_path = os.sep.join([engine.temp, self.project_id, script_part]) if package_missing_flag \
                                else self.package
        if not os.path.exists(script_part) :
            file_pattern = re.compile(r'\w+\.\w+')
            if file_pattern.search(script_part) is not None :
                self.script = self.script.replace(script_part, candidate_script_path, 1)
        return missing_files


class Message:
    def __init__(self, mconfig):
        self.worker = mconfig['worker']
        self.project_id = mconfig['project_id']
        self.task_name = mconfig['task_name']
        self.status = int(mconfig['status'])
        self.message = mconfig['message']
        self.outputs = []
        if 'outputs' in mconfig :
            if 'file' in mconfig['outputs'] and type(mconfig['outputs']['file']) == list :
                for item in mconfig['outputs']['file'] :
                    self.outputs.append(DataFile(item, is_output=True))
            elif 'file' in mconfig['outputs'] :
                self.outputs.append(DataFile(mconfig['outputs']['file'], is_output=True))
            if 'folder' in mconfig['outputs'] and type(mconfig['outputs']['folder']) == list :
                for item in mconfig['outputs']['folder'] :
                    self.outputs.append(DataFile(item, is_output=True, is_folder=True))
            elif 'folder' in mconfig['outputs'] :
                self.outputs.append(DataFile(mconfig['outputs']['folder'], is_output=True, is_folder=True))


class Command:
    def __init__(self, cconfig, system):
        self.project_name = cconfig.get('project', None)
        self.project_id = cconfig.get('project_id', None)
        self.email = cconfig.get('email', None)
        self.command = cconfig['command']
        self.projects = {}
        if self.project_name is not None and self.email is not None and self.project_id is None:
            candidates = system.findProjectByName(self.project_name, self.email)
            for candidate in candidates:
                self.projects[candidate.project_id] = candidate
        elif self.project_id is not None :
            if self.project_id in system.projects :
                self.projects[self.project_id] = system.projects[self.project_id]


class Notice :
    """
    this is the class for internal info transfer, message collector will get this from the message queue, and
    then create message xml to send back to controller.
    """
    def __init__(self, project_id, task_name, message="", status=0, outputs=None):
        """
        status : 0 if success, -1 if error
        """
        self.project_id = project_id
        self.task_name = task_name
        self.message = message
        self.status = status
        self.outputs = outputs

    def createMessage(self, engine_name):
        """
        address : the address of current engine
        """
        message_xml = xmlwitch.Builder()
        with message_xml.message() :
            message_xml.project_id(self.project_id)
            message_xml.worker(engine_name)
            message_xml.task_name(self.task_name)
            message_xml.status(str(self.status))
            message_xml.message(self.message)
            if self.outputs is not None and len(self.outputs) > 0 :
                with message_xml.outputs() :
                    for item in self.outputs :
                        if item.is_output:
                            datatype = 'output'
                        elif item.is_input:
                            datatype = 'data'
                        else :
                            datatype = 'na'
                        if item.is_folder:
                            message_xml.folder(item.path, id=item.id, type=datatype)
                        else :
                            message_xml.file(item.path, id=item.id, type=datatype)
        return str(message_xml)