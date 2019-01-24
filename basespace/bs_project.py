from basespace import basespace
from basespace.utils import api_response, api_collection


def get_list(project_name=None):
    return basespace.get_list("projects", "Name", project_name)


def get_details(project_id):
    return basespace.get_details("projects", project_id)


def get_app_results_list(project_id):
    return api_collection("v1pre3/projects/%s/appresults" % project_id)
