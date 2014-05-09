# Large Scale Machine Learning Platform

---

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


