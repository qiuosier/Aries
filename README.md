# Aries

Aries is a collection of python modules providing shortcuts to tasks like running background tasks, creating spreadsheets, configuring logging, etc.

[![Build Status](https://travis-ci.org/qiuosier/Aries.svg?branch=master)](https://travis-ci.org/qiuosier/Aries)
[![Coverage Status](https://coveralls.io/repos/github/qiuosier/Aries/badge.svg?branch=master)](https://coveralls.io/github/qiuosier/Aries?branch=master)
[![codebeat badge](https://codebeat.co/badges/f7217133-f495-455d-8808-d2cdaf46bc60)](https://codebeat.co/projects/github-com-qiuosier-aries-master)

Aries includes:
* Database(`db.py`) module to access SQLite database files.
* Excel(`excel.py`) module to create, read and modify Microsoft Excel spreadsheets.
* Files(`files.py`) module to handle JSON, Markdown and temporary files with templates.
* [Outputs(`outputs.py`)](docs/outputs.md) module for logging and capturing outputs.
* [Storage(`storage.py`)](docs/storage.md) module to provide a unified interface for access data (including data on Google Cloud Storage) and object-oriented shortcuts for listing, copying, deleting files/folder.
* String(`string.py`) module to provide enhanced String types, as well as FileName, Base64String.
* Task(`tasks.py`) module to manage asynchronous/background functions and commands.
* Web(`web.py`) module to access web APIs.
