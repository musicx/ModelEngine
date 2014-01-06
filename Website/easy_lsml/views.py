from django.shortcuts import render, render_to_response
#from django.http import HttpResponse
from django.template import RequestContext
from easy_lsml.models import EasyProject
from easy_lsml.forms import EasyProjectForm
import xmlwitch

# Create your views here.
def index(request) :
    context = RequestContext(request)
    form = EasyProjectForm()
    context_dict = {'title' : "Large Scale Machine Learning", 'form' : form}
    return render_to_response('easy_lsml/index.html', context_dict, context)

def generate_xml(request) :
    context = RequestContext(request)
    if request.method == 'POST' :
        form = EasyProjectForm(request.POST)

        if form.is_valid() :
            project = form.save(commit=False)
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
                            if project.has_evaluation:
                                project_xml.stage_script("evaluation script") #TODO
                                project_xml.stage_package("evaluation package")
                                project_xml.skip_stage('')
                                project_xml.stage_input('')
                                with project_xml.stage_output() :
                                    project_xml.file("gainchart.csv", type="output")
            return render_to_response('easy_lsml/xml.html', {'xml':str(project_xml)}, context)
        else :
            print form.errors
    else :
        form = EasyProjectForm()
        context_dict = {'title' : "Large Scale Machine Learning", 'form' : form}
    return render_to_response('easy_lsml/index.html', context_dict, context)
