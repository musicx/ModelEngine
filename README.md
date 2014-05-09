# Large Scale Machine Learning Platform

Large scale machine learning platform is used as a intermediate layer between the modelers and the training servers in IDI environment.

## Installation

1. Clone the project to the target machine.
2. Make sure following packages are installed in your python environment
    * paramiko
    * xmltodict
    * xmlwitch
3. Add the new machine to `configuration/engine_config.xml`. And add it to the same file on the master machine as well.
4. Run `python project/modelengine/start.py`. Restart the running version, if any, on the master.

## Kick off a project

### Project configuration

An example of the project configuration is `input/ato_test_mep.xml`.

A project is comprised of multiple stages, running sequencially. And a stage consists of multiple tasks, running parallelly. It also includes input and output configuration on task, stage and project level.

The configuration for input / output is in the following format:

+ File : The file element contains the file path to the specific file. Either absolute path or relative path are acceptable. If relative path is used, the base folder is the working folder for each task individually. The file element consists of 2 attributes, `id` and `type`.
    * `id` should be unique on project level. It is used for file reference among tasks.
        * If `id` is missing, the file cannot be referred / used in later tasks.
    * `type` can be chosen from `data` and `output`. `data` means the file will be copied to the `temp` folder as intermediary file, and will be deleted if you set `clean_after_success` to `yes`. `output` means the file will be copied to the `output` folder when task is finished, and will not be deleted.
        * If `type` is missing, the file will be treated as `output` in default.

+ Folder : The folder element is acceptable only for output part. It means the whole folder will be copied back to the master output folder.

The configuration for a task includes several parts:

+ Name: I would recommend to use a self-explaning name, so when you receive the notification email, you can understand what is done easily.

+ Job type: The platform currently supports 4 types of jobs. The job types each engine supported is written in the `engine_config.xml`.
    * `mb` : task requires modelbuilder
    * `sas` : task requires sas
    * `python` : task requires python
    * `r` : task requires r

+ Script : A script is the command for the task with all the parameters defined. All the input must be defined except the input / output file names. File names can be replaced with ids defined in the `input` and `output` fields.
> For example, `%base` on line 19 in file `input/ato_test_mep.xml` will be replaced with the file path defined on line 7 after it is tested for accessibility.
> And `%tmp1` on line 57 will be replaced with the actual file path after file defined on line 24 is copied from python job server to modelbuilder job server.

+ Package : A package can be the path to a simple script file, a zip file, or empty if you use linux built-in commands. It will be copied to the working directory of the worker machines, unzipped there and be triggered with the script defined above.

+ Task input : If the file has already been referred from project input or stage input, it doesn't need to be declared more than once.

+ Task output : The output files / folders must be specified. Because the working directory is dynamically created, absolute path is not recommand unless you want to specify where the output files are stored in the script element.

When all the tasks within a stage is done, a stage task will be triggered. The stage task will be run on the master server, and ONLY the master server.
> In `input/ato_test_mep.xml` the only stage task is a R based script, this is because we knew that the master is a server that can run r scripts. However, this is not recommended for system maintanance.
> A possible better solutions is to run the `merge.py` as the stage task of the second stage. And run the r-based evaluation task in the final stage as a task.

### Start a project

After the configuration file is ready. Simply copy the `.xml` file to the `taskpool` folder of the master server to start the project.

If any task / stage is done, or error happened during task script execution, a email will be sent to the email address in the project configuration file.

### Track a project

Currently there is no easy way to track a project. However, you can always check the `delievery` folder for the subdirectory with the name of your project and a latest timestamp.

In the project delievery folder, all parsed configuration and messages among servers are stored. You can check which task is currently running and what the earlier messages were.

However, if any error occurs on the platform itself, the user will not receive any update. And only the platform starter can check the platform output to find the bug. I'll work more on this later.