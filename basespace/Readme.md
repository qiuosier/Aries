## Credentials
BaseSpace credentials are required to access data in BaseSpace.
The BaseSpace package is designed to have the BaseSpace credentials stored in a json file. The json file should contain the credentials like the following:
```
{
    "access_token": "xxxxxxxxxxxxxxxxxxx",
    "client_id": "xxxxxxxxxxxxxxxxxxx",
    "client_secret": "xxxxxxxxxxxxxxxxxxx"
}
```
This package uses the system environment variable "BASESPACE_CREDENTIALS" to import the dictionary storing the credentials.

The "BASESPACE_CREDENTIALS" system environment variable must be set before using the BaseSpace package. For Example:
```
os.environ["BASESPACE_CREDENTIALS"] = "../abc/basespace.json"
```
