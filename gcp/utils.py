from google.oauth2 import service_account


def load_credentials(file_path):
    return service_account.Credentials.from_service_account_file(file_path)
