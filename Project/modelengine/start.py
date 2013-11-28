import glob
import logging
from multiprocessing import Process, Queue
import os
import subprocess
import sys
import xmltodict
import time
from project import *
from engine import *
from threading import Thread, Event

__author__ = 'yijiliu'

##############################
# This file is run when the engine starts.
# It reads the system level configuration.
# It starts the timer to check the taskpool.
# Then it runs in the background as a daemon.
#
# The timer will read all files in the taskpool,
# parse if it is a model project or a command,
# move the task file to the garbage,
# then init the project stage by stage, or run the command.
#
# Date: 2013-Oct-25
##############################


class TaskScanner(Thread):
    def __init__(self, event, whole_system, out_queue, ref_queue):
        Thread.__init__(self)
        self.stopped = event
        self.system = whole_system
        self.interval = whole_system.interval
        self.engine = whole_system.engine
        self.task_queue = out_queue
        self.message_queue = ref_queue

    def run(self):
        while not self.stopped.isSet():
            self.stopped.wait(self.interval)
            self.checkxml()

    def checkxml(self):
        taskfiles = glob.glob(os.path.join(self.engine.taskpool, '*.xml'))
        for filename in taskfiles:
            with open(filename) as fn:
                lines = fn.read()
            try:
                doc = xmltodict.parse(lines)
            except Exception:
                logging.error("Please make sure the task configuration file has a proper format : {0}".format(filename))
                #TODO: find email address in the xml and send error message to him/her.
                continue

            if "project" in doc:
                project = Project(doc['project'], self.system)
                inner_name = "{0}_{1}".format(project.name, long(time.time()))
                while inner_name in self.system.projects:
                    inner_name = "{0}_{1}".format(project.name, long(time.time()))
                self.system.projects[inner_name] = project
                project.inner_name = inner_name
                self.checkFolder(project.inner_name)
                self.moveTask(filename, project.inner_name)
                project.nextTasks()

            elif "worktask" in doc:
                worktask = WorkTask(doc['worktask'])
                self.checkFolder(worktask.project_id)
                self.moveTask(filename, worktask.project_id)
                missing_list = worktask.check(self.engine)
                # copy the input file from remote to local
                finish_events = []
                for item in missing_list :
                    finished = Event()
                    fetcher = Fetcher(self.engine, self.system.controller.address, item.path,
                                      self.engine.temp, finished)
                    fetcher.start()
                    finish_events.append(finished)
                worker_process = WorkerProcess(worktask.script, self.message_queue, finish_events, worktask.package)
                self.engine.worktasks[worktask.project_id].append(worker_process)
                worker_process.start()

            elif "command" in doc:
                command = Command(doc['command'])
                if command.project_name is not None:
                    self.moveTasks(filename, command.projects)
                else:
                    self.moveTask(filename, None)
                    #TODO: generate same command xml and send it to all clients
                # do the project level command, such as stop task, stage or shut the proj down
                # close engine
                if command.command == "shutdown":
                    if command.project_name is None:
                        self.system.stopper.set()
                    else:
                        # TODO: because there is no python API to kill thread or subprocess, set the project to deactivate to stop FURTHER actions
                        # TODO: however, for multiprocessing.Process, p.terminate() can be used!
                        for project in command.projects.itervalues():
                            project.deactivate = True

            elif "message" in doc:
                message = Message(doc['message'])
                # message status meaning:
                # -1 : error, email project owner
                # 0 : success, update task status
                if message.status == 0 :
                    output_files = self.system.projects[message.project_id].getOutput(message.task_name)
                    finish_events = []
                    for item in output_files :
                        finished = Event()
                        fetcher = Fetcher(self.engine, message.worker, item.path,
                                          self.engine.output, finished)
                        fetcher.start()
                        finish_events.append(finished)
                    statusUpdater = StatusUpdater(self.system.projects[message.project_id], message.task_name,
                                                  message.status, finish_events)
                    statusUpdater.start()

            else:
                logging.warning("unknown root command found in the task configuration file : {0}".format(filename))
                #TODO: find email address in the xml and send error message to him/her.


    def moveTask(self, filename, inner_name):
        basename = os.path.basename(filename)
        timestamp = long(time.time())
        if inner_name is not None :
            target = os.sep.join((self.engine.delivery, inner_name,
                                  "del_{0}_{1}".format(timestamp, basename)))
        else :
            target = os.sep.join((self.engine.delivery, "del_{0}_{1}".format(timestamp, basename)))
        try:
            os.rename(filename, target)
        except Exception:
            logging.warning("Task cannot be moved to delivery folder!\nTrying to remove the xml suffix automatically!")
            try:
                os.rename(filename, filename[:-4])
            except Exception:
                logging.error("Task renaming cannot be done! Previlege may needed! Exception Raised!")
                raise IOError


    def moveTasks(self, filename, inner_names):
        basename = os.path.basename(filename)
        timestamp = long(time.time())
        error_count = 0
        for inner_name in inner_names :
            target = os.sep.join((self.engine.delivery, inner_name,
                                  "del_{0}_{1}".format(timestamp, basename)))
            try :
                shutil.copyfile(filename, target)
            except Exception :
                error_count += 1
        if error_count == len(inner_names) :
            try:
                os.rename(filename, filename[:-4])
            except Exception:
                logging.error("Task renaming cannot be done! Previlege may needed! Exception Raised!")
                raise IOError
        else :
            os.remove(filename)


    def checkFolder(self, name):
        temp = os.sep.join((self.engine.temp, name))
        output = os.sep.join((self.engine.output, name))
        delivery = os.sep.join((self.engine.delivery, name))
        if not os.path.exists(temp) :
            os.makedirs(temp)
        if not os.path.exists(output) :
            os.makedirs(output)
        if not os.path.exists(delivery) :
            os.makedirs(delivery)


class Distributer(Thread) :
    def __init__(self, cur_engine, source_folder, task_name, xml_content, target_folder, is_local=False):
        Thread.__init__(self)
        self.engine = cur_engine
        self.target = target_folder
        self.source = source_folder
        self.task_name = task_name
        self.xml = xml_content
        self.is_local = is_local
        #self.setDaemon(True)

    def run(self) :
        filename = self.generateTaskFile()
        res = self.deliver(filename)
        if res < 0 :
            #TODO: COPY ERROR
            print "copy error"

    def generateTaskFile(self):
        base_name = "{0}_{1}.xml".format(self.task_name, long(time.time()))
        file_path = os.sep.join([self.source, base_name])
        with open(file_path, 'w') as fn:
            fn.write(self.xml)
            fn.write("\n")
        return file_path

    def deliver(self, filename):
        target_path = os.sep.join((self.target, os.path.basename(filename)))
        if self.is_local :
            logging.info("move to taskpool of local server")
            #os.rename(filename, target_path)
            shutil.copyfile(filename, target_path)
        else :
            logging.info("copy to remote server: {0}".format(self.target))
            runningOutput = subprocess.check_output(["scp -i {0} -q {1} {2}".format(self.engine.rsa_key, filename, self.target) + "; exit 0"],
                                                    stderr=subprocess.STDOUT, shell=True)
            if runningOutput != "" :
                logging.error("cannot copy file to remote location")
                return -1
        return 0


class Fetcher(Thread) :
    def __init__(self, cur_engine, source_address, file_path, target_folder, finished):
        Thread.__init__(self)
        self.engine = cur_engine
        self.source = source_address
        self.filename = file_path
        self.target = target_folder
        self.finished = finished

    def run(self) :
        target_path = self.target + os.sep if not self.target.endswith(os.sep) else self.target
        source_path = "{0}@{1}:{2}".format(self.engine.username, self.source, self.filename)
        logging.info("fetch file {0} from remote server {1}".format(self.filename, self.source))
        runningOutput = subprocess.check_output(["scp -i {0} -q {1} {2}".format(self.engine.rsa_key, source_path, target_path) + "; exit 0"],
                                                stderr=subprocess.STDOUT, shell=True)
        if runningOutput != "" :
            #TODO: COPY ERROR
            print "copy error"
        else :
            self.finished.set()


class MessageCollector(Thread) :
    def __init__(self, in_queue, cur_engine, target_engine, is_local):
        Thread.__init__(self)
        self.engine = cur_engine
        self.notice_queue = in_queue
        self.is_local = is_local
        if self.is_local :
            self.target = target_engine.taskpool
        else :
            self.target = "{0}@{1}:{2}".format(target_engine.username, target_engine.address, target_engine.taskpool)
        self.setDaemon(True)

    def run(self):
        while True :
            notice = self.notice_queue.get()
            #send to controller
            xml_content = notice.createMessage(self.engine.address)
            message_sender = Distributer(self.engine, os.sep.join([self.engine.delivery, notice.project_id]),
                                         notice.project_id + "_message", xml_content, self.target, self.is_local)
            message_sender.start()
            self.notice_queue.task_done()


class StatusUpdater(Thread) :
    def __init__(self, project, taskname, status, events=None):
        Thread.__init__(self)
        self.project = project
        self.taskname = taskname
        self.status = status
        self.events = events

    def run(self):
        if self.events is not None :
            for event in self.events :
                if not event.isSet() :
                    event.wait(1)
        self.project.updateStatus(self.taskname, self.status)


class WorkerThread(Thread) :
    def __init__(self, script, out_queue, events=None, package=None):
        Thread.__init__(self)
        self.script = script
        self.message_queue = out_queue
        self.events = events
        self.package = package

    def run(self):
        if self.events is not None :
            for item in self.events :
                while not item.isSet() :
                    item.wait(1)
        try :
            if sys.platform.startswith("win") :
                runningOutput = subprocess.check_output(["cmd /C " + self.script + "& exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
            else :
                runningOutput = subprocess.check_output([self.script + "; exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
        except Exception as err:
            runningOutput = err.message
        self.message_queue.put(runningOutput)


class WorkerProcess(Process) :
    def __init__(self, script, out_queue, events=None, package=None, cur_dir=""):
        Process.__init__(self)
        self.script = script
        self.message_queue = out_queue
        self.events = events
        self.package = package
        self.dir = cur_dir

    def run(self):
        if self.events is not None:
            for item in self.events:
                while not item.isSet():
                    item.wait(1)
        if self.package is not None :
            filename = os.path.basename(self.package)
            unzip = ""
            if filename.endswith(".zip") :
                unzip = "unzip -q "
            elif filename.endswith(".tar.gz") :
                unzip = "tar xzf "
            elif filename.endswith(".tar.bz2") :
                unzip = "tar xjf "
            elif filename.endswith(".gz") :
                unzip = "gunzip -q "
            elif filename.endswith(".bz2") :
                unzip = "bzip2 -dq "
            if unzip != "" :
                zip_res = subprocess.call(unzip + os.sep.join([self.dir, filename]), stderr=subprocess.STDOUT, shell=True)
                if zip_res != "" :
                    #TODO Error log / stop task / notify controller of the failure
                    self.message_queue.put(zip_res)
                    return
        try :
            if sys.platform.startswith("win") :
                runningOutput = subprocess.check_output(["cmd /C " + self.script + "& exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
            else :
                runningOutput = subprocess.check_output([self.script + "; exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
        except Exception as err:
            runningOutput = err.message
        self.message_queue.put(runningOutput)


if __name__ == '__main__':
    config_path = r'c:\work\python\modelengine\configuration\engine_config.xml'
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(processName)s : %(threadName)s - %(message)s')
    system = System(config_path)
    system.stopper = Event()
    stopper = Event()
    message_queue = Queue()
    timer = TaskScanner(stopper, system, message_queue)
    timer.start()
    message_collector = MessageCollector(message_queue)
    message_collector.start()
    while not system.stopper.isSet():
        system.stopper.wait(15)
    stopper.set()
