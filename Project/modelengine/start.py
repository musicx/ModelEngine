import glob
import logging
from multiprocessing import Process, JoinableQueue
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
    def __init__(self, stopper_event, current_system, out_queue):
        Thread.__init__(self)
        self.stopped = stopper_event
        self.system = current_system
        self.interval = current_system.interval
        self.engine = current_system.engine
        self.message_queue = out_queue

    def run(self):
        while not self.stopped.isSet():
            self.stopped.wait(self.interval)
            self.checkTaskPool()

    def checkTaskPool(self):
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
                try :
                    project = Project(doc['project'], self.system)
                    self.system.projects[project.project_id] = project
                    self.checkFolder(project.project_id)
                    project.nextTasks()
                except ValueError as err:
                    #TODO: find email address in the xml and send error message to him/her.
                    pass
                finally:
                    self.moveTask(filename, project.project_id)

            elif "worktask" in doc:
                worktask = WorkTask(doc['worktask'])
                self.checkFolder(worktask.project_id)
                missing_list = worktask.checkMissing(self.engine)
                self.moveTask(filename, worktask.project_id)
                # copy the input file from remote to local
                finish_events = []
                for item in missing_list :
                    finished = Event()
                    fetcher = Fetcher(self.engine, self.system.controller, item.path,
                                      self.engine.temp, finished, item.is_folder)
                    fetcher.start()
                    finish_events.append(finished)
                worker_process = WorkerProcess(worktask.project_id, worktask.task_name, worktask.script, worktask.outputs,
                                               self.message_queue, finish_events, worktask.package)
                # maintain a worktask dict for different projects
                self.engine.worktasks[worktask.project_id].append(worker_process)
                worker_process.start()

            elif "command" in doc:
                command = Command(doc['command'], self.system)
                if len(command.projects) != 0:
                    self.moveTasks(filename, command.projects)
                else:
                    self.moveTask(filename, None)
                if command.project_id is None and self.system.controller.address == self.engine.address :
                    # generate same user created command xml and send it to all clients except self (controller)
                    controller_command = xmlwitch.Builder()
                    with controller_command.command() :
                        if command.project_name is not None :
                            controller_command.project(command.project_name)
                        if command.email is not None :
                            controller_command.email(command.email)
                        controller_command.command(command.command)
                    for project_id, project in command.projects.iteritems() :
                        for engine in self.system.engines.itervalues() :
                            if engine.address == self.system.controller.address :
                                continue
                            distributor = Distributer(self.engine, os.sep.join([self.engine.delivery, project_id]),
                                                      "dup_" + command.command, str(controller_command),
                                                      "{0}@{1}:{2}".format(engine.username, engine.address, engine.taskpool))
                            distributor.start()
                    if len(command.projects) == 0 :
                        for engine in self.system.engines.itervalues() :
                            if engine.address == self.system.controller.address :
                                continue
                            distributor = Distributer(self.engine, self.engine.delivery,
                                                      "dup_" + command.command, str(controller_command),
                                                      "{0}@{1}:{2}".format(engine.username, engine.address, engine.taskpool))
                            distributor.start()
                # do the project level command, such as stop task, stage or shut the proj down
                # close engine
                if command.command == "shutdown":
                    if len(command.projects) == 0:
                        self.system.stopper.set()
                    else:
                        # because there is no python API to kill thread or subprocess,
                        # set the project to deactivate to stop FURTHER actions.
                        # however, for multiprocessing.Process, p.terminate() can be used!
                        for project_id, project in command.projects.iteritems():
                            project.deactivate = True
                            worker_processes = self.engine.worktasks.pop(project_id, {})
                            for worker_process in worker_processes :
                                if worker_process.is_alive() :
                                    worker_process.terminate()
                elif command.command == "clean" :
                    for project_id in command.projects :
                        worker_processes = self.engine.worktasks.pop(project_id, {})
                        for worker_process in worker_processes :
                            if worker_process.is_alive() :
                                worker_process.terminate()
                        project = self.system.projects.pop(project_id, None)
                        if project is None :
                            continue
                        # TODO: remove the temp / delievery / output folder if necessary


            elif "message" in doc:
                message = Message(doc['message'])
                # message status meaning:
                # -1 : error, email project owner
                # 0 : success, update task status
                if message.status == 0 :
                    finish_events = []
                    for item in message.outputs :
                        finished = Event()
                        source_path = item.path
                        target_folder = os.sep.join([self.engine.output, message.project_id])
                        if item.is_folder and not os.path.exists(item.relative_path):
                            target_folder = os.sep.join([target_folder, item.relative_path])
                        fetcher = Fetcher(self.engine, self.system.engines[message.worker], source_path, target_folder, finished)
                        fetcher.start()
                        finish_events.append(finished)
                    statusUpdater = StatusUpdater(self.system.projects[message.project_id], message.task_name,
                                                  message.status, finish_events)
                    statusUpdater.start()
                elif message.status == -1 :
                    logging.error(message.message)


            else:
                logging.warning("unknown root command found in the task configuration file : {0}".format(filename))
                #TODO: find email address in the xml and send error message to him/her.


    def moveTask(self, filename, project_id):
        """
        logic is to put project level file to its own delivery folder,
        and to put system level file to root delivery folder
        if not successful, remove the ".xml" suffix
        if still not successful, raise IOError
        """
        basename = os.path.basename(filename)
        timestamp = long(time.time())
        if project_id is not None :
            target = os.sep.join((self.engine.delivery, project_id,
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

    def moveTasks(self, filename, project_ids):
        """
        logic is to first copy the file to each project delivery folder, then remove it if any success
        if all failed, remove the ".xml" suffix and leave it in the taskpool folder
        if failed again, raise IOError
        """
        basename = os.path.basename(filename)
        timestamp = long(time.time())
        error_count = 0
        for project_id in project_ids :
            target = os.sep.join((self.engine.delivery, project_id,
                                  "del_{0}_{1}".format(timestamp, basename)))
            try :
                shutil.copyfile(filename, target)
            except Exception :
                error_count += 1
        if error_count == len(project_ids) :
            try:
                os.rename(filename, filename[:-4])
            except Exception:
                logging.error("Task renaming cannot be done! Previlege may needed! Exception Raised!")
                raise IOError
        else :
            os.remove(filename)

    def checkFolder(self, project_id):
        temp = os.sep.join((self.engine.temp, project_id))
        output = os.sep.join((self.engine.output, project_id))
        delivery = os.sep.join((self.engine.delivery, project_id))
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
    def __init__(self, local_engine, remote_engine, source_path, target_folder, finished, is_folder=False):
        Thread.__init__(self)
        self.local = local_engine
        self.remote = remote_engine
        self.source = source_path
        self.target = target_folder
        self.finished = finished
        self.is_folder = is_folder
        self.is_local = self.local.address == self.remote.address

    def run(self) :
        # add move mechanism if it is local fetcher
        target_path = self.target + os.sep if not self.target.endswith(os.sep) else self.target
        if self.is_local :
            source_path = self.source
            logging.info("fetch file {0} from local server".format(self.source))
            if source_path.find(self.local.temp) >= 0 :
                # in the temp folder, move to output folder
                if self.is_folder :
                    if not os.path.exists(target_path) :
                        os.makedirs(target_path)
                    os.rename(source_path, target_path)
                else :
                    os.rename(source_path, os.sep.join([target_path, os.path.basename(source_path)]))
            else :
                # not in temp folder, copy to output folder
                if self.is_folder :
                    if os.path.exists(target_path) :
                        shutil.rmtree(target_path, ignore_errors=True)
                    shutil.copytree(source_path, target_path)
                else :
                    shutil.copy(source_path, target_path)
            runningOutput = ""
        else :
            source_path = "{0}@{1}:{2}".format(self.remote.username, self.remote.address, self.source)
            logging.info("fetch file {0} from remote server {1}".format(self.source, self.remote.address))
            if self.is_folder :
                runningOutput = subprocess.check_output(["scp -i {0} -q -r {1} {2}".format(self.local.rsa_key, source_path, target_path) + "; exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
            else :
                runningOutput = subprocess.check_output(["scp -i {0} -q {1} {2}".format(self.local.rsa_key, source_path, target_path) + "; exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
        if runningOutput != "" :
            #TODO: COPY ERROR
            print "copy error"
        else :
            self.finished.set()


class MessageCollector(Thread) :
    """
    this is a daemon thread used to receive notices from other threads and processes
    a notice will be sent to controller as Message to update project status
    """
    def __init__(self, notice_queue, source_engine, target_engine):
        """
        notice_queue: the JoinableQueue used to receive notices
        source_engine: the Engine of source machine / CURRENT engine
        target_engine: the Engine of target machine / CONTROLLER engine
        """
        Thread.__init__(self)
        self.engine = source_engine
        self.notice_queue = notice_queue
        self.is_local = source_engine.address == target_engine.address
        if self.is_local :
            self.target = target_engine.taskpool
        else :
            self.target = "{0}@{1}:{2}".format(target_engine.username, target_engine.address, target_engine.taskpool)
        self.setDaemon(True)

    def run(self):
        while True :
            notice = self.notice_queue.get()
            #send to controller
            xml_content = notice.createMessage(self.engine.name)
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
    def __init__(self, project_id, task_name, script, outputs, out_queue, events=None, package=None):
        """
        wait for all the Events in events are set, then start the script
        before script is run, un-package any zip files if given
        then send the output to message_queue as Notice
        """
        Thread.__init__(self)
        self.project_id = project_id
        self.task_name = task_name
        self.script = script
        self.outputs = outputs
        self.message_queue = out_queue
        self.events = events
        self.package = package

    def run(self):
        if self.events is not None:
            for item in self.events:
                while not item.isSet():
                    item.wait(1)
        if self.package is not None :
            unzip = ""
            if self.package.endswith(".zip") :
                unzip = "unzip -q "
            elif self.package.endswith(".tar.gz") :
                unzip = "tar xzf "
            elif self.package.endswith(".tar.bz2") :
                unzip = "tar xjf "
            elif self.package.endswith(".gz") :
                unzip = "gunzip -q "
            elif self.package.endswith(".bz2") :
                unzip = "bzip2 -dq "
            if unzip != "" :
                zip_res = subprocess.call(unzip + self.package, stderr=subprocess.STDOUT, shell=True)
                if zip_res != "" :
                    #TODO Error log / stop task / notify controller of the failure
                    self.message_queue.put(Notice(self.project_id, self.task_name, zip_res, -1))
                    return
        try :
            if sys.platform.startswith("win") :
                runningOutput = subprocess.check_output(["cmd /C " + self.script + "& exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
            else :
                runningOutput = subprocess.check_output([self.script + "; exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
            has_error = (runningOutput.find("error") >= 0) * -1
        except Exception as err:
            runningOutput = err.message
            has_error = -1
        self.message_queue.put(Notice(self.project_id, self.task_name, runningOutput, has_error, self.outputs))


class WorkerProcess(Process) :
    def __init__(self, project_id, task_name, script, outputs, out_queue, events=None, package=None):
        """
        wait for all the Events in events are set, then start the script
        before script is run, un-package any zip files if given
        then send the output, together with the detail of output files to message_queue as Notice
        """
        Process.__init__(self)
        self.project_id = project_id
        self.task_name = task_name
        self.script = script
        self.outputs = outputs
        self.message_queue = out_queue
        self.events = events
        self.package = package

    def run(self):
        if self.events is not None:
            for item in self.events:
                while not item.isSet():
                    item.wait(1)
        if self.package is not None :
            unzip = ""
            if self.package.endswith(".zip") :
                unzip = "unzip -q "
            elif self.package.endswith(".tar.gz") :
                unzip = "tar xzf "
            elif self.package.endswith(".tar.bz2") :
                unzip = "tar xjf "
            elif self.package.endswith(".gz") :
                unzip = "gunzip -q "
            elif self.package.endswith(".bz2") :
                unzip = "bzip2 -dq "
            if unzip != "" :
                zip_res = subprocess.call(unzip + self.package, stderr=subprocess.STDOUT, shell=True)
                if zip_res != "" :
                    #TODO Error log / stop task / notify controller of the failure
                    self.message_queue.put(Notice(self.project_id, self.task_name, zip_res, -1))
                    return
        try :
            if sys.platform.startswith("win") :
                runningOutput = subprocess.check_output(["cmd /C " + self.script + "& exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
            else :
                runningOutput = subprocess.check_output([self.script + "; exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True)
            has_error = (runningOutput.find("error") >= 0) * -1
        except Exception as err:
            runningOutput = err.message
            has_error = -1
        self.message_queue.put(Notice(self.project_id, self.task_name, runningOutput, has_error, self.outputs))


class EmailNotifier(Thread) :
    def __init__(self, project_name, email, message):
        Thread.__init__(self)
        subject = "Update on project {0}".format(project_name)
        self.script = "printf {0} | mail -s {1} {2}".format(repr(message), repr(subject), email)

    def run(self):
        if not sys.platform.startswith("win") :
            result = subprocess.check_call([self.script + "; exit 0"], stderr=subprocess.STDOUT, shell=True)
            #TODO : ERROR handle


if __name__ == '__main__':
    config_path = r'c:\work\python\modelengine\configuration\engine_config.xml'
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(processName)s : %(threadName)s - %(message)s')
    system = System(config_path)
    system.stopper = Event()        # used to kill the system if shutdown command is found
    stopper = Event()               # used to kill the daemon task scanner
    message_queue = JoinableQueue()         # the queue shared by multiple processes for result communication
    timer = TaskScanner(stopper, system, message_queue)
    timer.start()
    message_collector = MessageCollector(message_queue, system.engine, system.controller)
    message_collector.start()
    while not system.stopper.isSet():
        system.stopper.wait(15)
    stopper.set()
