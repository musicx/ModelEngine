from django.shortcuts import render, render_to_response
from django.template import RequestContext
from django.db.models import Q
from easy_lsml.models import EasyProject, EasyProjectLog
from easy_lsml.forms import EasyProjectForm
import xmlwitch
import os

# Create your views here.
def new_project(request) :
    context = RequestContext(request)
    project_id = request.GET.get('pid', '')
    if project_id != "" :
        try :
            project_detail = EasyProject.objects.get(id=project_id)
        except EasyProject.DoesNotExist :
            form = EasyProjectForm()
        else :
            form = EasyProjectForm(instance=project_detail)
    else :
        form = EasyProjectForm()
    context_dict = {'form' : form}
    return render_to_response('easy_lsml/new.html', context_dict, context)

def generate_xml(request) :
    context = RequestContext(request)
    if request.method == 'POST' :
        form = EasyProjectForm(request.POST)

        if form.is_valid() :
            project = form.save(commit=True)
            project_xml = xmlwitch.Builder()
            with project_xml.project() :
                project_xml.name(project.name)
                project_xml.email(project.owner)
                with project_xml.project_input() :
                    project_xml.file(project.input_data_train, id="train")
                    project_xml.file(project.input_data_test, id="test")
                    project_xml.file(project.model_variable_list, id="model_var")
                    project_xml.file(project.preserve_variable_list, id="const_var")
                project_xml.clean_after_success('')
                with project_xml.stages() :
                    if project.has_preproc :
                        with project_xml.stage():
                            project_xml.name("Pre-Processing")
                            with project_xml.tasks() :
                                with project_xml.task() :
                                    project_xml.name("woe generation and simple variable selection")
                                    project_xml.job_type("python")
                                    project_xml.script("preprocessing script") #TODO
                                    project_xml.package("preprocessing package path")
                                    project_xml.skip_task('')
                                    project_xml.task_input('')
                                    with project_xml.task_output() :
                                        project_xml.file("model_vars_woed.txt", id="model_var_pro")
                                        project_xml.file("transformed_train.csv", id="train_trans")
                                        project_xml.file("transformed_test.csv", id="test_trans")
                            if project.preproc_clustering :
                                project_xml.stage_script("clustering script") #TODO
                                project_xml.stage_package("clustering package")
                                project_xml.skip_stage('')
                                project_xml.stage_input('')
                                with project_xml.stage_output() :
                                    project_xml.file("model_vars_clus.txt", id="model_var_clus")
                    if project.has_nn or project.has_tree :
                        with project_xml.stage() :
                            project_xml.name("Modeling")
                            with project_xml.tasks():
                                if project.has_sas :
                                    with project_xml.task() :
                                        project_xml.name("Build logistic regression model")
                                        project_xml.job_type("sas")
                                        project_xml.script("sas script") # TODO
                                        project_xml.package("sas package path")
                                        project_xml.skip_task('')
                                        project_xml.task_input('')
                                        with project_xml.task_output() :
                                            project_xml.file("sas_spec.tar.gz", type="output" if project.sas_output_spec else "data")
                                            project_xml.file("sas_scored_train.csv", id="sas_score_train", type="output" if project.sas_output_scored else "data")
                                            project_xml.file("sas_scored_test.csv", id="sas_score_test", type="output" if project.sas_output_scored else "data")
                                if project.has_nn :
                                    with project_xml.task() :
                                        project_xml.name("Build neural network model")
                                        project_xml.job_type("mb")
                                        project_xml.script("Modelbuilder script") # TODO
                                        project_xml.package("modelbuilder script path")
                                        project_xml.skip_task('')
                                        project_xml.task_input('')
                                        with project_xml.task_output() :
                                            project_xml.file("nn_spec.tar.gz", type="output" if project.nn_output_spec else "data")
                                            project_xml.file("nn_scored_train.csv", id="nn_score_train", type="output" if project.nn_output_scored else "data")
                                            project_xml.file("nn_scored_test.csv", id="nn_score_test", type="output" if project.nn_output_scored else "data")
                                            if project.nn_sensitivity :
                                                project_xml.file("nn_sensitivity.tar.gz", type="output")
                                if project.has_tree :
                                    with project_xml.task():
                                        project_xml.name("Build TreeNet model")
                                        project_xml.job_type("r")
                                        project_xml.script("TreeNet script") # TODO
                                        project_xml.package("TreeNet script path")
                                        project_xml.skip_task('')
                                        project_xml.task_input('')
                                        with project_xml.task_output() :
                                            project_xml.file("tree_spec.tar.gz", type="output" if project.tree_output_spec else "data")
                                            project_xml.file("tree_scored_train.csv", id="tree_score_train", type="output" if project.tree_output_scored else "data")
                                            project_xml.file("tree_scored_test.csv", id="tree_score_test", type="output" if project.tree_output_scored else "data")
                                            if project.tree_output_importance :
                                                project_xml.file("tree_importance.tar.gz", type="output")
                                if project.has_custom :
                                    with project_xml.task() :
                                        project_xml.name("Build custom model")
                                        project_xml.job_type(project.custom_type)
                                        project_xml.script(project.custom_script) # TODO
                                        project_xml.package(project.custom_package)
                                        project_xml.skip_task('')
                                        with project_xml.task_input() :
                                            custom_inputs = project.custom_input.split(';')
                                            for custom_ind in xrange(len(custom_inputs)):
                                                project_xml.file(custom_inputs[custom_ind], id="cus_in{}".format(custom_ind))
                                        with project_xml.task_output() :
                                            custom_outputs = project.custom_output.split(';')
                                            for custom_ind in xrange(len(custom_outputs)):
                                                if custom_outputs[custom_ind].rfind('.') < custom_outputs[custom_ind].rfind(os.sep) :
                                                    project_xml.folder(custom_outputs[custom_ind], type="output", id="cus_out{}".format(custom_ind))
                                                else :
                                                    project_xml.file(custom_outputs[custom_ind], type="output", id="cus_out{}".format(custom_ind))
                            if project.has_evaluation:
                                project_xml.stage_script("evaluation script") #TODO
                                project_xml.stage_package("evaluation package")
                                project_xml.skip_stage('')
                                project_xml.stage_input('')
                                with project_xml.stage_output() :
                                    project_xml.file("gainchart.csv", type="output")
            log = EasyProjectLog()
            log.name = project.name
            log.owner = project.owner
            log.config = project
            log.status = 0
            log.output = '<p class="lead">Please wait for the notification emails and then come back for output details</p>'
            log.save()
            context_dict = {}
            return render_to_response('easy_lsml/auto.html', context_dict, context)
        else :
            print form.errors
    form = EasyProjectForm()
    context_dict = {'form' : form}
    return render_to_response('easy_lsml/new.html', context_dict, context)

def index(request) :
    context = RequestContext(request)
    if request.method == 'POST' :
        print request.POST
        keywords = request.POST['keyword'].strip().split(" ")
        q = Q()
        for keyword in keywords :
            q = q | Q(name__icontains=keyword) | Q(owner__icontains=keyword)
        project_list = EasyProjectLog.objects.filter(q).order_by('-time')[:20]
    else:
        project_list = EasyProjectLog.objects.order_by('-time')[:20]
    context_dict = {'projects': project_list}
    ind = 0
    for project in project_list :
        project.ind = ind
        project.pid = project.config.id
        ind += 1
        if project.status == 0 :
            project.run_status = 'running'
        elif project.status == 1:
            project.run_status = 'success'
        elif project.status == 2:
            project.run_status = 'failed'
        elif project.status == 3:
            project.run_status = 'canceled'
        else :
            project.run_status = 'unknown'
    return render_to_response('easy_lsml/index.html', context_dict, context)

def cheat(request) :
    context = RequestContext(request)
    if request.method == 'POST' :
        form = EasyProjectForm(request.POST)
        print request.POST
        if form.is_valid() :
            project = form.save(commit=True)
            log = EasyProjectLog()
            log.name = project.name
            log.owner = project.owner
            log.config = project
            log.status = request.POST['status']
            log.output = request.POST['output']
            log.save()
        else :
            print form.errors
    form = EasyProjectForm()
    form.fields['name'].initial = "test_project"
    form.fields['owner'].initial = "yijiliu@ebay.com"
    form.fields['input_data_train'].initial = "/training/data/path/train.csv"
    form.fields['input_data_test'].initial = "/test/data/path/test.csv"
    form.fields['model_variable_list'].initial = "/model/variable/list.txt"
    form.fields['preserve_variable_list'].initial = "/preserve/variable/list.txt"
    default_output = """<h4>Output from SAS Logistic Regression module</h4> <table class="table table-condensed table-striped"> <tr> <td>Specs</td> <td><a href="/static/file/sas/specs.tar.gz">zip file</a></td> </tr> <tr> <td>Gain Chart</td> <td><a href="/static/file/sas/gainchart.xls">gainchart.xls</a></td> </tr> </table> <hr/> <h4>Output from ModelBuilder Neural Network module</h4> <table class="table table-condensed table-striped"> <tr> <td>Specs</td> <td><a href="/static/file/nn/specs.tar.gz">zip file</a></td> </tr> <tr> <td>Gain Chart</td> <td><a href="/static/file/nn/gainchart.xls">gainchart.xls</a></td> </tr> <tr> <td>Sensitivity</td> <td><a href="/static/file/nn/sensitivity.txt">variables.txt</a></td> </tr> </table> <hr/> <h4>Output from R TreeNet module</h4> <table class="table table-condensed table-striped"> <tr> <td>Specs</td> <td><a href="/static/file/tree/specs.tar.gz">zip file</a></td> </tr> <tr> <td>Importance</td> <td><a href="/static/file/tree/importance.txt">variables.txt</a></td> </tr> </table> """
    context_dict = {'form' : form, 'output' : default_output, 'status' : 1}
    return render_to_response('easy_lsml/cheat.html', context_dict, context)

def command(request) :
    context = RequestContext(request)
    project_id = request.GET.get('pid', '')
    if project_id != "" :
        action = request.GET.get('action', '')
        try :
            project_detail = EasyProject.objects.get(id=project_id)
            project_log_detail = EasyProjectLog.objects.get(config=project_detail)
        except EasyProject.DoesNotExist :
            print "cannot find easyproject"
        except EasyProjectLog.DoesNotExist :
            print "cannot find easyprojectlog"
        else :
            if action.lower() == 'cancel' :
                project_log_detail.status = 3
                project_log_detail.output = '<p class="lead">This project is canceled</p>'
                project_log_detail.save()
    context_dict = {}
    return render_to_response('easy_lsml/auto.html', context_dict, context)
