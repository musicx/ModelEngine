from django import forms
from easy_lsml.models import EasyProject, EasyProjectLog


class EasyProjectForm(forms.ModelForm) :
    name = forms.CharField(max_length=128, label='Project Name',
            help_text="Please give your project a name",
            widget=forms.TextInput(attrs={'placeholder': 'Project Name'}))
    owner = forms.CharField(max_length=128, label="Email",
            help_text="Please provide your email for notification",
            widget=forms.TextInput(attrs={'placeholder': 'XXX@ebay.com'}))
    input_data_train = forms.CharField(max_length=4096, label="Training Data",
            help_text="Please specify the path to your DEV dataset",
            widget=forms.TextInput(attrs={'placeholder': '/path/to/your/file/train.csv',
                                          'class': 'input-xxlarge'}))
    input_data_test = forms.CharField(max_length=4096, label="Test Data",
            help_text="Please specify the path to your OOT dataset",
            widget=forms.TextInput(attrs={'placeholder': '/path/to/your/file/test.csv',
                                          'class': 'input-xxlarge'}))
    delimiter = forms.CharField(max_length=8, initial=',', label="Data Delimiter",
            help_text="Please specify the delimiter of your data file",
            widget=forms.TextInput(attrs={'class': 'input-mini'}))
    target_var = forms.CharField(max_length=128, initial='is_bad', label="Target Variable",
            help_text="Please specify the target variable name")
    weight_var = forms.CharField(max_length=128, initial='unit_weight', label="Weight Variable",
            help_text="Please specify the training weight variable name")
    model_variable_list = forms.CharField(max_length=4096, label="Model Variables List",
            help_text="Please provide the path to the list file of modeling variables",
            widget=forms.TextInput(attrs={'placeholder': '/path/to/your/file/model_vars.lst',
                                          'class': 'input-xxlarge'}))
    preserve_variable_list = forms.CharField(max_length=4096, label="Analysis Variable List",
            help_text="Please provide the path to the list file of analysis variables, eg loss, segment, etc",
            widget=forms.TextInput(attrs={'placeholder': '/path/to/your/file/eval_vars.lst',
                                          'class': 'input-xxlarge'}))

    has_preproc = forms.BooleanField(initial=True, label='Apply Pre-Processing', required=False,
            help_text="Choose if you want variable pre-processing applied")
    preproc_woe = forms.BooleanField(initial=False, label="Generate WoE Mapping", required=False,
            help_text="Choose if you want woe transformed variables generated")
    preproc_zscl = forms.BooleanField(initial=False, label="Apply z-scaling Before WoE", required=False,
            help_text="Choose if you want z-scaling transformed variables generated")
    preproc_woe_zscl = forms.BooleanField(initial=True, label="Apply z-scaling After WoE", required=False,
            help_text="Choose if you want z-scaling transformed variables generated after applying WoE transformation")
    preproc_min_iv = forms.FloatField(initial=0.01, label="Minimum IV",
            help_text="Minimum IV acceptable for variables filtering",
            widget=forms.TextInput(attrs={'class': 'input-mini'}))
    preproc_max_missing = forms.FloatField(initial=0.99, label="Maximum Missing Rate",
            help_text="Maximum missing rate acceptable for variable filtering",
            widget=forms.TextInput(attrs={'class': 'input-mini'}))
    preproc_clustering = forms.BooleanField(initial=False, label="Apply Variable Clustering", required=False,
            help_text="Choose if you want variable selection with clustering algorithm")
    preproc_clusters = forms.IntegerField(initial=200, label="Number of Clusters", required=False,
            help_text="The number of clusters that the algorithm should look for",
            widget=forms.TextInput(attrs={'class': 'input-mini'}))

    has_sas = forms.BooleanField(initial=True, label="Build SAS Logistic Regression Model", required=False,
            help_text="Choose if you want to build a logistic regression model")
    sas_max_variable = forms.IntegerField(initial=50, label="Maximum of Variables",
            help_text="Maximum of variables that the spec should contain")
    sas_max_corr = forms.FloatField(initial=0.5, label="Maximum of Correlation",
            help_text="Maximum of correlation that is allowed among chosen variables")
    sas_output_scored = forms.BooleanField(initial=False, label="Output Scored Dataset", required=False,
            help_text='Choose if you want to output the scored datasets')
    sas_output_spec = forms.BooleanField(initial=True, label="Output Model Specs", required=False,
            help_text='Choose if you want to output the logistic regression model specs')

    has_nn = forms.BooleanField(initial=True, label='Build Neural Network Model', required=False,
            help_text='Choose if you want to build a neural network model')
    nn_validation_variable = forms.CharField(max_length=128, initial="setid", label="Validation Variable",
            help_text='The variable used to determine training and validation data parts')
    nn_hidden_node = forms.CharField(max_length=20, initial=20, label="Number of Nodes",
            help_text='Number of nodes in hidden layer, could be multiple numbers separated with comma')
    nn_sensitivity = forms.BooleanField(initial=False, label='Apply Sensitivity Analysis', required=False,
            help_text='Choose if you want sensitivity analysis for variable selection')
    nn_output_scored = forms.BooleanField(initial=True, label='Output Scored Dataset', required=False,
            help_text='Choose if you want to output the scored datasets')
    nn_output_spec = forms.BooleanField(initial=True, label='Output Model Specs', required=False,
            help_text='Choose if you want to output the neural network model specs')

    has_tree = forms.BooleanField(initial=True, label='Build TreeNet Model', required=False,
            help_text='Choose if you want to build a TreeNet model')
    tree_number = forms.IntegerField(initial=2000, label='Number of Trees',
            help_text='The number of trees in the final model')
    tree_depth = forms.IntegerField(initial=3, label='Depth of Single Tree',
            help_text='The depth of each single tree in the model')
    tree_leaf_size = forms.IntegerField(initial=300, label='Min Obs in Leaf Node',
            help_text='The minimum observations in each leaf node of a tree')
    tree_sample_rate = forms.FloatField(initial=0.6, label='Sample Rate',
            help_text='Sample rate used during tree generation')
    tree_learning_rate = forms.FloatField(initial=0.05, label='Learning Rate',
            help_text='Learning rate of the algorithm')
    tree_output_spec = forms.BooleanField(initial=True, label='Output Model Spec', required=False,
            help_text='Choose if you want to output the TreeNet model spec')
    tree_output_scored = forms.BooleanField(initial=True, label='Output Scored Dataset', required=False,
            help_text='Choose if you want to output the scored datasets')
    tree_output_importance = forms.BooleanField(initial=False, label='Output Variable Importance', required=False,
            help_text='Choose if you want to output the variable importance for variable selection')

    has_custom = forms.BooleanField(initial=False, label="Build Your Own Model", required=False,
            help_text="Choose if you want to build model with your own package")
    custom_package = forms.CharField(max_length=4096, label="Package", required=False,
            help_text="The path to your package on server phxaidiedge3, zipped file is supported",
            widget=forms.TextInput(attrs={'placeholder': '/path/to/your/package/package.tar.gz',
                                          'class': 'input-xxlarge'}))
    custom_script = forms.CharField(max_length=4096, label='Command Line Script', required=False,
            help_text="The command line to use your package. Python and Shell are supported on all servers, others please specify",
            widget=forms.TextInput(attrs={'placeholder': 'exeutable command.file argument1 argument2',
                                          'class': 'input-xxlarge'}))
    JOB_TYPES = (('python', 'Python'),
                 ('mb', 'Model Builder'),
                 ('r', 'R'),
                 ('sas', 'SAS'))
    custom_type = forms.ChoiceField(label="Type of Command", choices=JOB_TYPES, initial='python', required=False,
            help_text="Please specify which tool you will use to train your model")
    custom_input = forms.CharField(max_length=65536, label="Input Files", required=False,
            help_text="All the input files should be accessable from phxaidiedge3. Files should be seperated with ';'",
            widget=forms.TextInput(attrs={'placeholder': '/path/to/your/input.file;/path/to/your/another/input.file',
                                          'class': 'input-xxlarge'}))
    custom_output = forms.CharField(max_length=65536, label="Output Files / Folder", required=False,
            help_text="The path to the output file or folder in relative path to your working directory",
            widget=forms.TextInput(attrs={'placeholder': 'path/to/your/output/folder',
                                          'class': 'input-xxlarge'}))

    has_evaluation = forms.BooleanField(initial=True, label='Generate Gain Charts', required=False,
            help_text='Choose if you want to generate unified model performance report')
    eva_baseline = forms.CharField(max_length=4096, label='Baseline Scores', required=False,
            help_text='Baseline score names to be compared with',
            widget=forms.TextInput(attrs={'placeholder': 'old_score'}))
    eva_category = forms.CharField(max_length=4096, label='Category Variables', required=False,
            help_text='By what category variable combinations the gain charts should be generated, eg, segment',
            widget=forms.TextInput(attrs={'placeholder': 'segment_variable'}))
    eva_variable = forms.CharField(max_length=4096, label='Inspectation Variables', required=False,
            help_text='The interested variables whose catch rate will be computed, eg, net_loss',
            widget=forms.TextInput(attrs={'placeholder': 'net_or_gross_loss'}))
    eva_unit_weight = forms.CharField(max_length=128, label='Unit Weight', initial='unit_weight', required=False,
            help_text='The variable name for unit weight')
    eva_dollar_weight = forms.CharField(max_length=128, label='Dollar Weight', initial='dollar_weight', required=False,
            help_text='The variable name for dollar weight')

    class Meta :
        model = EasyProject


class EasyProjectLogForm(forms.ModelForm) :
    default_config = """<project> <name>test_project</name> <email>yijiliu@ebay.com</email> <project_input> <file id="train">path_to_train_data_set</file> <file id="test">path_to_test_data_set</file> <file id="model_var">model_variable_list</file> <file id="const_var">preserve_variable_list</file> </project_input> <clean_after_success></clean_after_success> <stages> <stage> <name>Pre-Processing</name> <tasks> <task> <name>woe generation and simple variable selection</name> <job_type>python</job_type> <script>preprocessing script</script> <package>preprocessing package path</package> <skip_task></skip_task> <task_input></task_input> <task_output> <file id="model_var_pro">model_vars_woed.txt</file> <file id="train_trans">transformed_train.csv</file> <file id="test_trans">transformed_test.csv</file> </task_output> </task> </tasks> </stage> <stage> <name>Modeling</name> <tasks> <task> <name>Build logistic regression model</name> <job_type>sas</job_type> <script>sas script</script> <package>sas package path</package> <skip_task></skip_task> <task_input></task_input> <task_output> <file type="output">sas_spec.tar.gz</file> <file type="data" id="sas_score_train">sas_scored_train.csv</file> <file type="data" id="sas_score_test">sas_scored_test.csv</file> </task_output> </task> <task> <name>Build neural network model</name> <job_type>mb</job_type> <script>Modelbuilder script</script> <package>modelbuilder script path</package> <skip_task></skip_task> <task_input></task_input> <task_output> <file type="output">nn_spec.tar.gz</file> <file type="output" id="nn_score_train">nn_scored_train.csv</file> <file type="output" id="nn_score_test">nn_scored_test.csv</file> </task_output> </task> <task> <name>Build TreeNet model</name> <job_type>r</job_type> <script>TreeNet script</script> <package>TreeNet script path</package> <skip_task></skip_task> <task_input></task_input> <task_output> <file type="output">tree_spec.tar.gz</file> <file type="output" id="tree_score_train">tree_scored_train.csv</file> <file type="output" id="tree_score_test">tree_scored_test.csv</file> </task_output> </task> </tasks> <stage_script>evaluation script</stage_script> <stage_package>evaluation package</stage_package> <skip_stage></skip_stage> <stage_input></stage_input> <stage_output> <file type="output">gainchart.csv</file> </stage_output> </stage> </stages> </project> """
    default_output = """<h4>Output from SAS Logistic Regression module</h4> <table class="table table-condensed table-striped"> <tr> <td>Specs</td> <td><a href="/static/file/sas/specs.tar.gz">zip file</a></td> </tr> <tr> <td>Gain Chart</td> <td><a href="/static/file/sas/gainchart.xls">gainchart.xls</a></td> </tr> </table> <hr/> <h4>Output from ModelBuilder Neural Network module</h4> <table class="table table-condensed table-striped"> <tr> <td>Specs</td> <td><a href="/static/file/nn/specs.tar.gz">zip file</a></td> </tr> <tr> <td>Gain Chart</td> <td><a href="/static/file/nn/gainchart.xls">gainchart.xls</a></td> </tr> <tr> <td>Sensitivity</td> <td><a href="/static/file/nn/sensitivity.txt">variables.txt</a></td> </tr> </table> <hr/> <h4>Output from R TreeNet module</h4> <table class="table table-condensed table-striped"> <tr> <td>Specs</td> <td><a href="/static/file/tree/specs.tar.gz">zip file</a></td> </tr> <tr> <td>Importance</td> <td><a href="/static/file/tree/importance.txt">variables.txt</a></td> </tr> </table> """

    name = forms.CharField(max_length=128, initial='test_project')
    owner = forms.CharField(max_length=128, initial='yijiliu@ebay.com')
    status = forms.IntegerField(initial=1)
    config = forms.CharField(max_length=65536, initial=default_config,
            widget=forms.Textarea(attrs={'rows':"10"}))
    output = forms.CharField(max_length=65536, initial=default_output,
            widget=forms.Textarea(attrs={'rows':"10"}))
    class Meta :
        model = EasyProjectLog
        fields = {"name", "owner", "config", "output", "status"}

