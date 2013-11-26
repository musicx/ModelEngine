Input folder contains all the project level user specified files.
Files in this folder can be stored anywhere in the controller machine, and
 be given the full path in the project configuration file.

files describing raw data
1. preserve_var.txt : the raw variable names which should not be used as
                      variables in model training process but need to be
                      saved in the final evaluation data.
                      record_id and bad_tagging should be included in this
                      file as well.
2. model_var.txt : only the raw variable names which should be considered
                   during the modeling process.
