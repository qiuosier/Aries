from . import basespace
from .utils import api_response, api_collection


def get_list(project_name=None):
    return basespace.get_list("projects", "Name", project_name)


def get_details(project_id):
    return basespace.get_details("projects", project_id)


def get_app_results_list(project_id):
    return api_collection("v1pre3/projects/%s/appresults" % project_id)


def get_samples(project_name):
    """Gets a list of samples for a project.

    Args:
        project_name (str): The name of the project.

    Returns: A list of sample items (dictionaries, as in the BaseSpace API response).

    """
    projects = get_list(project_name)
    href_list = []
    href_samples = None
    samples = []

    for project in projects:
        if project.get("Name") == project_name:
            href = project.get("Href")
            if href:
                href_list.append(href)

    for href in href_list:
        response = api_response(href)
        if response:
            href_samples = response.get("HrefSamples")

        if href_samples:
            samples.extend(api_collection(href_samples))

    return samples
