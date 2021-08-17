Mate
====

.. image:: https://circleci.com/gh/ausseabed/mate.svg?style=svg
    :target: https://circleci.com/gh/ausseabed/mate
    :alt: CircleCI Status

* Code: `GitHub repo <https://github.com/ausseabed/mate>`_
* License: Apache 2.0

Installation
------------

**Note:** Mate requires Python v3.6+, the following commands are written assuming
:bash:`pip` has been setup as an alias to a Python 3 based installation. If this is not the case please use :bash:`pip3` in place of :bash:`pip` below.

Clone source code::

    git clone https://github.com/ausseabed/mate.git
    cd mate

Install dependencies::

    pip install -r requirements.txt

Install application::

    pip install -e .


Command Line Application
------------------------
Mate includes a simple command line application. Usage can be displayed as follows::

    %> hyo2.mate -h

    usage: hyo2.mate [-h] -i INPUT [-o OUTPUT]

    optional arguments:
      -h, --help            show this help message and exit
      -i INPUT, --input INPUT
                            Path to input QA JSON file
      -o OUTPUT, --output OUTPUT
                            Path to output QA JSON file. If not provided will be
                            printed to stdout.

An example command line is shown below::

    hyo2.mate --input tests/test_data/input.json --output tests/test_data/test_out.json


Testing
-------

Unit tests can be run as follows::

    python -m pytest --cov=hyo2.mate --cov-report=html  tests/

**Note:** Unit tests will fail if the test data has not been downloaded (see following section)

Test Data
---------

A collection of data has been uploaded to an AWS S3 bucket to support testing and development of the Mate application. This data requires AWS credentials to download (contact development team).

Once AWS access credentials have been obtained the test data can be downloaded using the `AWS CLI <https://aws.amazon.com/cli/>`_.

**Note:** This data is approximately 20Gb in size

Windows instructions
********************

Set environment variables for AWS::

    set AWS_ACCESS_KEY_ID=<AWS Access Key Id>
    set AWS_SECRET_ACCESS_KEY=<AWS Access Key Secret>

Download data from S3 into current folder::

    aws s3 sync s3://ausseabed-marine-qa-data/mate-test-data/ ./tests/test_data_remote


Linux, MacOS instructions
*************************

Set environment variables for AWS::

    export AWS_ACCESS_KEY_ID=<AWS Access Key Id>
    export AWS_SECRET_ACCESS_KEY=<AWS Access Key Secret>

Download data from S3 into current folder::

    aws s3 sync s3://ausseabed-marine-qa-data/mate-test-data/ ./tests/test_data_remote
