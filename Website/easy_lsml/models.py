from django.db import models

# Create your models here.
class EasyProject(models.Model) :
    name = models.CharField(max_length=128)
    owner = models.CharField(max_length=128)
    input_data_train = models.CharField(max_length=4096)
    input_data_test = models.CharField(max_length=4096)
    delimiter = models.CharField(max_length=8)
    target_var = models.CharField(max_length=128)
    weight_var = models.CharField(max_length=128)
    model_variable_list = models.CharField(max_length=4096)
    preserve_variable_list = models.CharField(max_length=4096)

    has_preproc = models.BooleanField(default=True)
    preproc_woe = models.BooleanField(default=False)
    preproc_zscl = models.BooleanField(default=False)
    preproc_woe_zscl = models.BooleanField(default=True)
    preproc_min_iv = models.FloatField(default=0.01)
    preproc_max_missing = models.FloatField(default=0.99)
    preproc_clustering = models.BooleanField(default=True)
    preproc_clusters = models.IntegerField(default=500)

    has_sas = models.BooleanField(default=True)
    sas_max_variable = models.IntegerField(default=50)
    sas_max_corr = models.FloatField(default=0.5)
    sas_output_scored = models.BooleanField(default=False)
    sas_output_spec = models.BooleanField(default=True)

    has_nn = models.BooleanField(default=True)
    nn_validation_variable = models.CharField(max_length=128, default="setid")
    nn_hidden_node = models.CharField(max_length=20, default=20)
    nn_sensitivity = models.BooleanField(default=False)
    nn_output_scored = models.BooleanField(default=False)
    nn_output_spec = models.BooleanField(default=True)

    has_tree = models.BooleanField(default=True)
    tree_number = models.IntegerField(default=2000)
    tree_depth = models.IntegerField(default=3)
    tree_leaf_size = models.IntegerField(default=300)
    tree_sample_rate = models.FloatField(default=0.6)
    tree_learning_rate = models.FloatField(default=0.05)
    tree_output_spec = models.BooleanField(default=True)
    tree_output_scored = models.BooleanField(default=False)
    tree_output_importance = models.BooleanField(default=False)

    JOB_TYPES = (('python', 'Python'),
                 ('mb', 'Model Builder'),
                 ('r', 'R'),
                 ('sas', 'SAS'))
    has_custom = models.BooleanField(default=False)
    custom_package = models.CharField(max_length=4096)
    custom_script = models.CharField(max_length=4096)
    custom_type = models.CharField(max_length=32, default='python', choices=JOB_TYPES)
    custom_input = models.CharField(max_length=65536)
    custom_output = models.CharField(max_length=65536)

    has_evaluation = models.BooleanField(default=True)
    eva_baseline = models.CharField(max_length=4096)
    eva_category = models.CharField(max_length=4096)
    eva_variable = models.CharField(max_length=4096)
    eva_unit_weight = models.CharField(max_length=128)
    eva_dollar_weight = models.CharField(max_length=128)


class EasyProjectLog(models.Model) :
    name = models.CharField(max_length=128)
    owner = models.CharField(max_length=128)
    config = models.ForeignKey(EasyProject)
    time = models.DateTimeField(auto_now_add=True)
    status = models.IntegerField(default=0)
    output = models.CharField(max_length=65536)

