import glob
from multiprocessing import Process, JoinableQueue, Event
import shutil
import logging
import subprocess
import sys
import paramiko

import distutils.dir_util
from scp import SCPClient
from project import *
from engine import *
from threading import Thread

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
        self.email_pattern = re.compile(r"[^>]+?@[^<]+")

    def run(self):
        while not self.stopped.is_set():
            self.stopped.wait(self.interval)
            self.checkTaskPool()

    def checkTaskPool(self):
        taskfiles = glob.glob(os.path.join(self.engine.taskpool, '*.xml'))
        for filename in taskfiles:
            logging.debug("file found: " + filename)
            with open(filename) as fn:
                lines = fn.read()
            try:
                doc = xmltodict.parse(lines)
            except Exception:
                logging.error("Please make sure the task configuration file has a proper format : {0}".format(filename))
                bad_email = self.findEmail(lines)
                message = "Your project configuration file {0} is not readable".format(filename)
                if bad_email is not None :
                    emailer = EmailNotifier("Unknown", bad_email, message, self.engine.delivery)
                    emailer.start()
                continue

            if "project" in doc:
                try :
                    project = Project(doc['project'], self.system)
                    logging.debug("project created " + project.project_id)
                    self.system.projects[project.project_id] = project
                    self.checkFolder(project.project_id)
                    project.nextTasks()
                    logging.debug("task generated for project " + project.project_id)
                    self.moveTask(filename, project.project_id)
                except ValueError as err:
                    self.moveTask(filename, None)
                    bad_email = self.findEmail(lines)
                    message = err.message + "\n"
                    message += "Your project configuration file {0} has a un-readable format".format(filename)
                    if bad_email is not None :
                        emailer = EmailNotifier("Unknown", bad_email, message, self.engine.delivery)
                        emailer.start()
                except IOError as err :
                    message = err.message + "\n"
                    message += "Your project configuration file {0} has a un-readable format".format(filename)
                    if project.email is not None :
                        emailer = EmailNotifier(project.name, project.email, message,
                                                os.sep.join([self.engine.delivery, project.project_id]))
                        emailer.start()
                    self.moveTask(filename, project.project_id)

            elif "worktask" in doc:
                worktask = WorkTask(doc['worktask'])
                logging.debug("worktask {0} created for project {1}".format(worktask.task_name, worktask.project_id))
                self.checkFolder(worktask.project_id, worktask.task_name)
                missing_list = worktask.checkMissing(self.engine)
                logging.debug("{0} missing files found for worktask ".format(len(missing_list)) + worktask.task_name)
                self.moveTask(filename, worktask.project_id)
                # copy the input file from remote to local
                finish_events = []
                for item in missing_list :
                    finished = Event()
                    fetcher = Fetcher(self.engine, self.system.controller, item.path,
                                      os.sep.join([self.engine.temp, worktask.project_id, worktask.task_name]),
                                      finished, is_folder=item.is_folder, is_output=item.is_output)
                    fetcher.start()
                    finish_events.append(finished)
                worker_process = WorkerProcess(worktask.project_id, worktask.task_name, worktask.script,
                                               os.sep.join([self.engine.temp, worktask.project_id, worktask.task_name]),
                                               worktask.outputs, self.message_queue, finish_events, worktask.package)
                # maintain a worktask dict for different projects
                self.engine.worktasks[worktask.project_id].append(worker_process)
                worker_process.start()
                logging.debug("worktask started! {0} : {1}".format(worktask.project_id, worktask.task_name))

            elif "command" in doc:
                command = Command(doc['command'], self.system)
                logging.debug("command created for {0}\nthe command is: {1}".format(command.projects, command.command))
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
                            distributer = Distributer(self.engine, os.sep.join([self.engine.delivery, project_id]),
                                                      engine, engine.taskpool, "dup_" + command.command, str(controller_command))
                            distributer.start()
                    if len(command.projects) == 0 :
                        for engine in self.system.engines.itervalues() :
                            if engine.address == self.system.controller.address :
                                continue
                            distributer = Distributer(self.engine, self.engine.delivery, engine, engine.taskpool,
                                                      "dup_" + command.command, str(controller_command))
                            distributer.start()
                    logging.debug("command has been distributed")
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
                        finish_message = "Your project {0} has finished, output files can be found in {1}".format(project.project_id,
                                                                                                                  os.sep.join([self.engine.output, project.project_id]))
                        emailer = EmailNotifier(project.project_id, project.email, finish_message,
                                                os.sep.join([self.engine.delivery, project.project_id]))
                        emailer.start()
                        # remove the temp / delievery / output folder if necessary
                        if project.clean_after :
                            shutil.rmtree(os.sep.join([self.engine.delivery, project.project_id]), ignore_errors=True)
                            shutil.rmtree(os.sep.join([self.engine.temp, project.project_id]), ignore_errors=True)


            elif "message" in doc:
                message = Message(doc['message'])
                logging.debug("message received for {0} : {1}".format(message.project_id, message.task_name))
                # message status meaning:
                # -1 : error, email project owner
                # 0 : success, update task status
                self.moveTask(filename, message.project_id)
                if message.status == 0 :
                    finish_events = []
                    for item in message.outputs :
                        finished = Event()
                        source_path = item.path
                        target_folder = os.sep.join((self.engine.output if item.is_output else self.engine.temp, message.project_id))
                        #if item.is_folder and not os.path.exists(item.folder_path):
                        #    target_folder = os.sep.join([target_folder, item.folder_path])
                        fetcher = Fetcher(self.engine, self.system.engines[message.worker], source_path,
                                          target_folder, finished, is_output=item.is_output, is_folder=item.is_folder)
                        fetcher.start()
                        finish_events.append(finished)
                    statusUpdater = StatusUpdater(self.system.projects[message.project_id], message.task_name,
                                                  message.status, finish_events)
                    statusUpdater.start()
                    emailer = EmailNotifier(message.project_id, self.system.projects[message.project_id].email,
                                            "Task {0} has successfully finished!".format(message.task_name),
                                            os.sep.join([self.system.engine.delivery, message.project_id]))
                    emailer.start()
                elif message.status == -1 :
                    logging.error(message.message)
                    emailer = EmailNotifier(message.project_id, self.system.projects[message.project_id].email,
                                            message.message, os.sep.join([self.system.engine.delivery, message.project_id]))
                    emailer.start()

            else:
                logging.warning("unknown root command found in the task configuration file : {0}".format(filename))
                os.rename(filename, filename[:-4])
                bad_email = self.findEmail(lines)
                message = "Your project configuration file {0} is not readable".format(filename)
                if bad_email is not None :
                    emailer = EmailNotifier("Unknown", bad_email, message, self.engine.delivery)
                    emailer.start()

    def findEmail(self, lines):
        match = self.email_pattern.search(lines)
        if match :
            return match.group()
        else :
            return None

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

    def checkFolder(self, project_id, task_name=None):
        if task_name is not None:
            temp = os.sep.join((self.engine.temp, project_id, task_name))
        else :
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
    def __init__(self, local_engine, source_folder, remote_engine, target_folder, task_name, xml_content):
        Thread.__init__(self)
        self.local = local_engine
        self.source = source_folder
        self.remote = remote_engine
        self.target = target_folder
        self.task_name = task_name
        self.xml = xml_content
        self.is_local = self.local.address == self.remote.address

    def run(self) :
        filename = self.generateTaskFile()
        res = self.deliver(filename)
        if res != 0 :
            logging.error("Copy Error! From {0}:{1} to {2}:{3}".format(self.local.address, self.source,
                                                                       self.remote.address, self.target))
            #TODO: send email to sys controller about this failure
            # possible solution: add a sys controller field in engine_config, and give every engine a copy of that field

    def generateTaskFile(self):
        base_name = "{0}_{1}.xml".format(self.task_name, long(time.time()))
        file_path = os.sep.join([self.source, base_name])
        with open(file_path, 'w') as fn:
            fn.write(self.xml)
            fn.write("\n")
        return file_path

    def deliver(self, filename):
        returncode = 0
        target_path = os.sep.join((self.target, os.path.basename(filename)))
        if self.is_local :
            logging.info("move to taskpool of local server")
            logging.debug("file copied from {0} to {1}".format(filename, target_path))
            #os.rename(filename, target_path)
            shutil.copyfile(filename, target_path)
        else :
            logging.info("copy to remote server: {0}".format(self.target))
            transport = None
            try :
                transport = self.create_transport()
                scp = SCPClient(transport)
                scp.put(filename, target_path)
                logging.debug("file copied from {0} to {1}".format(filename, self.target))
            except Exception as err:
                logging.error("SCP error : " + err.message)
                returncode = 1
            finally :
                if transport :
                    transport.close()
        return returncode

    def create_transport(self):
        hostkeytype = None
        hostkey = None
        try:
            host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        except IOError:
            try:
                # try ~/ssh/ too, e.g. on windows
                host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/ssh/known_hosts'))
            except IOError:
                logging.error('*** Unable to open host keys file')
                host_keys = {}
        if host_keys.has_key(self.remote.address):
            hostkeytype = host_keys[self.remote.address].keys()[0]
            hostkey = host_keys[self.remote.address][hostkeytype]
            logging.debug('Using host key of type {0}'.format(hostkeytype))
        # now, connect and use paramiko Transport to negotiate SSH2 across the connection
        logging.debug('Establishing SSH connection to: {0}:{1}'.format(self.remote.address, 22))
        transport = paramiko.Transport((self.remote.address, 22))
        transport.start_client()
        self.agent_auth(transport, self.local.username)
        if not transport.is_authenticated():
            logging.info('RSA key auth failed! Trying password login...')
            transport.connect(username=self.local.username, password="Oct$2013", hostkey=hostkey)
        return transport

    def agent_auth(self, transport, username):
        """
        Attempt to authenticate to the given transport using any of the private
        keys available from an SSH agent or from a local private RSA key file (assumes no pass phrase).
        """
        ki = None
        try:
            ki = paramiko.RSAKey.from_private_key_file(self.local.rsa_key)
        except Exception, e:
            logging.error('Failed loading: %s: %s' % (self.local.rsa_key, e.message))
        agent = paramiko.Agent()
        agent_keys = agent.get_keys() + (ki,)
        if len(agent_keys) == 0:
            return
        for key in agent_keys:
            logging.debug('Trying ssh-agent key %s' % key.get_fingerprint().encode('hex'))
            try:
                transport.auth_publickey(username, key)
                return
            except paramiko.SSHException as e:
                logging.error('authorization failed! ' + e.message)


class Fetcher(Thread) :
    def __init__(self, local_engine, remote_engine, source_path, target_folder,
                 finished, is_folder=False, is_output=False):
        Thread.__init__(self)
        self.local = local_engine
        self.remote = remote_engine
        self.source = source_path
        self.target = target_folder
        self.finished = finished
        self.is_folder = is_folder
        self.is_output = is_output
        self.is_local = self.local.address == self.remote.address

    def run(self) :
        # add move mechanism if it is local fetcher
        target_folder = self.target + os.sep if not self.target.endswith(os.sep) else self.target
        source_path = self.source
        #source_path = "{0}@{1}:{2}".format(self.remote.username, self.remote.address, self.source)
        if (source_path, target_folder) in self.local.fetchings :
            while self.local.fetchings[(source_path, target_folder)] :
                time.sleep(5)
            self.finished.set()
            return
        self.local.fetchings[(source_path, target_folder)] = True
        if self.is_local :
            logging.info("fetch file {0} from local server".format(self.source))
            if source_path.find(self.local.temp) >= 0:
                # in the temp folder, move to output folder
                if self.is_folder :
                    os.rename(source_path, target_folder)
                    logging.debug("move local folder from {0} to {1}".format(source_path, target_folder))
                else :
                    if not os.path.exists(target_folder) :
                        os.makedirs(target_folder)
                    target_path = target_folder + os.path.basename(source_path)
                    os.rename(source_path, target_path)
                    logging.debug("move local file from {0} to {1}".format(source_path, target_path))
            elif os.path.dirname(source_path) == os.path.dirname(target_folder) :
                logging.debug("source and target are the same : " + source_path)
            else :
                # not in temp folder, copy to output folder
                if not os.path.exists(target_folder) :
                    os.makedirs(target_folder)
                if self.is_folder :
                    #if os.path.exists(target_path) :
                    #    shutil.rmtree(target_path, ignore_errors=True)
                    distutils.dir_util.copy_tree(source_path, target_folder)
                    logging.debug("copy local folder from {0} to {1}".format(source_path, target_folder))
                else :
                    shutil.copy(source_path, target_folder)
                    logging.debug("copy local file from {0} to {1}".format(source_path, target_folder))
        else :
            logging.info("fetch file {0} from remote server {1}".format(self.source, self.remote.address))
            transport = None
            if not os.path.exists(target_folder) :
                os.makedirs(target_folder)
            try :
                transport = self.create_transport()
                scp = SCPClient(transport)
                #print "scp client established"
                scp.get(source_path, target_folder, recursive=self.is_folder)
                logging.debug("copy remote file from {0} to {1}".format(source_path, target_folder))
            except Exception as err:
                logging.error("SCP error : " + err.message)
            finally :
                if transport :
                    transport.close()

            #if self.is_folder :
            #    runningOutput = subprocess.check_output("scp -i {0} -q -r {1} {2}".format(self.local.rsa_key, source_path, target_path) + "; exit 0",
            #                                            stderr=subprocess.STDOUT, shell=True)
            #    logging.debug("copy remote folder from {0} to {1}".format(source_path, target_path))
            #else :
            #    #logging.debug("running command: " + "scp -i {0} -q {1} {2}".format(self.local.rsa_key, source_path, target_path))
            #    runningOutput = subprocess.check_output("scp -i {0} -q {1} {2}".format(self.local.rsa_key, source_path, target_path) + "; exit 0",
            #                                            stderr=subprocess.STDOUT, shell=True)
            #    #logging.debug("command done")
            #    logging.debug("copy remote file from {0} to {1}".format(source_path, target_path))
        self.local.fetchings[(source_path, target_folder)] = False
        self.finished.set()

    def create_transport(self):
        hostkeytype = None
        hostkey = None
        try:
            host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        except IOError:
            try:
                # try ~/ssh/ too, e.g. on windows
                host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/ssh/known_hosts'))
            except IOError:
                logging.error('*** Unable to open host keys file')
                host_keys = {}
        if host_keys.has_key(self.remote.address):
            hostkeytype = host_keys[self.remote.address].keys()[0]
            hostkey = host_keys[self.remote.address][hostkeytype]
            logging.debug('Using host key of type {0}'.format(hostkeytype))
        # now, connect and use paramiko Transport to negotiate SSH2 across the connection
        logging.debug('Establishing SSH connection to: {0}:{1}'.format(self.remote.address, 22))
        transport = paramiko.Transport((self.remote.address, 22))
        transport.start_client()
        self.agent_auth(transport, self.local.username)
        if not transport.is_authenticated():
            logging.info('RSA key auth failed! Trying password login...')
            transport.connect(username=self.local.username, password="Oct$2013", hostkey=hostkey)
        return transport

    def agent_auth(self, transport, username):
        """
        Attempt to authenticate to the given transport using any of the private
        keys available from an SSH agent or from a local private RSA key file (assumes no pass phrase).
        """
        ki = None
        try:
            ki = paramiko.RSAKey.from_private_key_file(self.local.rsa_key)
        except Exception, e:
            logging.error('Failed loading: %s: %s' % (self.local.rsa_key, e.message))
        agent = paramiko.Agent()
        agent_keys = agent.get_keys() + (ki,)
        if len(agent_keys) == 0:
            return
        for key in agent_keys:
            logging.debug('Trying ssh-agent key %s' % key.get_fingerprint().encode('hex'))
            try:
                transport.auth_publickey(username, key)
                return
            except paramiko.SSHException as e:
                logging.error('authorization failed! ' + e.message)


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
        self.local = source_engine
        self.remote = target_engine
        self.notice_queue = notice_queue
        self.is_local = source_engine.address == target_engine.address
        self.setDaemon(True)

    def run(self):
        while True :
            notice = self.notice_queue.get()
            logging.debug("notice received for {0} : {1}".format(notice.project_id, notice.message))
            #send to controller
            xml_content = notice.createMessage(self.local.name)
            message_sender = Distributer(self.local, os.sep.join([self.local.delivery, notice.project_id]),
                                         self.remote, self.remote.taskpool,
                                         "{0}_{1}_message_{2}".format(notice.project_id, notice.task_name, notice.status),
                                         xml_content)
            message_sender.start()
            logging.debug("notice {0} : {1} send via message".format(notice.project_id, notice.message))
            #self.notice_queue.task_done()


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
                if not event.is_set() :
                    event.wait(1)
        self.project.updateStatus(self.taskname, self.status)


class WorkerThread(Thread) :
    def __init__(self, project_id, task_name, script, work_folder, outputs, out_queue, events=None, package=None):
        """
        wait for all the Events in events are set, then start the script
        before script is run, un-package any zip files if given
        then send the output to message_queue as Notice
        """
        Thread.__init__(self)
        self.project_id = project_id
        self.task_name = task_name
        self.script = script
        self.work_folder = work_folder
        self.outputs = outputs
        self.message_queue = out_queue
        self.events = events
        self.package = "" if package is None else package
        if self.package :
            self.package = os.sep.join((self.work_folder, os.path.basename(self.package)))

    def run(self):
        if self.events is not None:
            for item in self.events:
                while not item.is_set():
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
                logging.debug("found zip file, try to unzip : " + self.package)
                try :
                    zip_res = subprocess.check_output([unzip + self.package], stderr=subprocess.STDOUT,
                                                      shell=True, cwd=self.work_folder)
                except subprocess.CalledProcessError as err :
                    self.message_queue.put(Notice(self.project_id, self.task_name,
                                                  "\n".join([err.message, err.output]), -1))
                    return
        try :
            if sys.platform.startswith("win") :
                runningOutput = subprocess.check_output(["cmd /C " + self.script + "& exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True,
                                                        cwd=self.work_folder)
            else :
                runningOutput = subprocess.check_output([self.script + "; exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True,
                                                        cwd=self.work_folder)
            has_error = 0
        except subprocess.CalledProcessError as err :
            runningOutput = "error occur: {0}\ncmd: {1}\nreturn code: {2}\noutput: {3}".format(err.message, err.cmd, err.returncode, err.output)
            print runningOutput
            has_error = -1
        except Exception as err:
            runningOutput = err.message
            has_error = -1
        notice = Notice(self.project_id, self.task_name, runningOutput, has_error, self.outputs)
        self.message_queue.put(notice)
        logging.debug("notice {0} : {1} send to collector".format(self.project_id, self.task_name))


class WorkerProcess(Process) :
    def __init__(self, project_id, task_name, script, work_folder, outputs, out_queue, events=None, package=None):
        """
        wait for all the Events in events are set, then start the script
        before script is run, un-package any zip files if given
        then send the output, together with the detail of output files to message_queue as Notice
        """
        Process.__init__(self)
        self.project_id = project_id
        self.task_name = task_name
        self.script = script
        self.work_folder = work_folder
        self.outputs = outputs
        self.message_queue = out_queue
        self.events = events
        self.package = "" if package is None else package
        if self.package :
            self.package = os.sep.join((self.work_folder, os.path.basename(self.package)))

    def run(self):
        if self.events is not None:
            for item in self.events:
                while not item.is_set():
                    item.wait(1)
        #print "all events set, command start"
        if self.package is not None and self.package != "" :
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
                #logging.debug("found zip file, try to unzip : " + self.package)
                print("found zip file, try to unzip : " + self.package)
                try :
                    zip_res = subprocess.check_output([unzip + self.package], stderr=subprocess.STDOUT,
                                                      shell=True, cwd=self.work_folder)
                except subprocess.CalledProcessError as err :
                    self.message_queue.put(Notice(self.project_id, self.task_name,
                                                  "\n".join([err.message, err.output]), -1))
                    return
        #logging.debug("running command is {0}".format(self.script))
        print("running command is {0}".format(self.script))
        try :
            if sys.platform.startswith("win") :
                runningOutput = subprocess.check_output(["cmd /C " + self.script + "& exit 0"],
                                                        stderr=subprocess.STDOUT, shell=True,
                                                        cwd=self.work_folder)
            else :
                runningOutput = subprocess.check_output([self.script], cwd=self.work_folder,
                                                        stderr=subprocess.STDOUT, shell=True)
            has_error = 0
        except subprocess.CalledProcessError as err :
            runningOutput = "error occur: {0}\ncmd: {1}\nreturn code: {2}\noutput: {3}".format(err.message, err.cmd, err.returncode, err.output)
            print runningOutput
            has_error = -1
        except Exception as err:
            runningOutput = err.message
            has_error = -1
        notice = Notice(self.project_id, self.task_name, runningOutput, has_error, self.outputs)
        self.message_queue.put(notice)
        #logging.debug("notice {0} : {1} send to collector".format(self.project_id, self.task_name))


class EmailNotifier(Thread) :
    def __init__(self, project_name, email, message, folder):
        Thread.__init__(self)
        subject = "Update on project {0}".format(project_name)
        message = "" if message is None else message
        temp_file_name = "email_{0}_{1}_{2}.msg".format(project_name, long(time.time()), random.randint(1,10000))
        temp_path = os.sep.join([folder, temp_file_name])
        with open(temp_path, 'w') as fn :
            fn.write(message)
        self.script = "cat {0} | mail -s {1} {2}".format(temp_path, repr(subject), email)

    def run(self):
        if not sys.platform.startswith("win") :
            try :
                result = subprocess.check_call([self.script + "; exit 0"], stderr=subprocess.STDOUT, shell=True)
            except subprocess.CalledProcessError as err :
                logging.error("Email cannot be send : " + err.message)


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
    while not system.stopper.is_set():
        system.stopper.wait(15)
    stopper.set()


