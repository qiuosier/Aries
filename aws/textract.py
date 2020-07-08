"""
https://docs.aws.amazon.com/textract/latest/dg/api-async-roles.html
The code in this file is modified based on the example code from AWS
https://docs.aws.amazon.com/textract/latest/dg/async-analyzing-with-sqs.html

"""

import boto3
import json
import time
import logging
from commons.Aries.storage import StorageFile

logger = logging.getLogger(__name__)


class NotificationChannel:
    sqs = boto3.client('sqs')
    sns = boto3.client('sns')

    def __init__(self, role_arn, queue_name, topic_arn=None):
        self.__queue_url = None
        self.role_arn = role_arn
        self.queue_name = queue_name
        self.topic_arn = topic_arn

    @property
    def queue_url(self):
        if not self.__queue_url:
            self.__queue_url = self.get_queue_url()
        return self.__queue_url

    def get_queue_url(self):
        return self.sqs.get_queue_url(QueueName=self.queue_name)['QueueUrl']

    def create(self, topic_name):
        # Create SNS topic
        topic_res = self.sns.create_topic(Name=topic_name)
        topic_arn = topic_res["TopicArn"]

        # Create SQS queue
        self.sqs.create_queue(QueueName=self.queue_name)
        queue_attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=['QueueArn']
        )['Attributes']
        queue_arn = queue_attrs['QueueArn']

        # Subscribe SQS queue to SNS topic
        self.sns.subscribe(
            TopicArn=topic_arn,
            Protocol='sqs',
            Endpoint=queue_arn
        )

        # Authorize SNS to write SQS queue
        policy = """{{
            "Version":"2012-10-17",
            "Statement":[
                {{
                "Sid":"MyPolicy",
                "Effect":"Allow",
                "Principal" : {{"AWS" : "*"}},
                "Action":"SQS:SendMessage",
                "Resource": "{}",
                "Condition":{{
                    "ArnEquals":{{
                    "aws:SourceArn": "{}"
                    }}
                }}
                }}
            ]
        }}""".format(queue_arn, topic_arn)
        self.sqs.set_queue_attributes(
            QueueUrl=self.queue_url,
            Attributes={
                'Policy': policy
            }
        )
        logger.debug("Created Topic ARN: %s" % topic_arn)
        logger.debug("Created Queue ARN: %s" % queue_arn)
        return topic_arn, queue_arn

    def to_dict(self):
        return {
            'RoleArn': self.role_arn,
            'SNSTopicArn': self.topic_arn
        }

    def receive_messages(self):
        return self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MessageAttributeNames=['ALL'],
            MaxNumberOfMessages=10
        )

    def delete_message(self, receipt_handle):
        return self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )

    def delete(self):
        self.sqs.delete_queue(QueueUrl=self.queue_url)
        self.sns.delete_topic(TopicArn=self.topic_arn)


class PDFAnalyzer:
    textract = boto3.client('textract')

    def __init__(self, input_uri, output_uri=None, process_type="detection", notification_channel=None):
        """

        Args:
            input_uri (str):
            process_type: "detection" or "analysis"
            output_uri (str):
            notification_channel (NotificationChannel):
        """
        self.input_uri = input_uri
        self.output_uri = output_uri
        self.storage_file = StorageFile(self.input_uri)
        self.notification_channel = notification_channel
        self.job_id = None
        self.process_type = str(process_type).lower()

    def start(self, **kwargs):
        if self.process_type == "analysis":
            func = self.textract.start_document_analysis
        else:
            func = self.textract.start_document_text_detection

        if self.notification_channel and "NotificationChannel" not in kwargs:
            kwargs["NotificationChannel"] = self.notification_channel.to_dict()
        response = func(
            DocumentLocation={'S3Object': {'Bucket': self.storage_file.bucket_name, 'Name': self.storage_file.prefix}},
            **kwargs
        )
        logger.info(response)
        self.job_id = str(response['JobId'])
        logger.debug('Started Job: ID=%s' % self.job_id)

    def request_results(self, **kwargs):
        if self.process_type == "analysis":
            func = self.textract.get_document_analysis
        else:
            func = self.textract.get_document_text_detection
        return func(**kwargs)

    def wait_for_results(self):
        job_finished = False
        while not job_finished:
            logger.debug("Waiting for analysis results...")
            time.sleep(10)
            if self.notification_channel:
                job_finished = self.check_notification()
            else:
                job_finished = self.check_status()
            # TODO: wait timeout?
        return self.get_results()

    def check_status(self):
        response = self.request_results(JobId=self.job_id)
        status = response.get("JobStatus")
        logger.debug(status)
        if status == "SUCCEEDED":
            return True
        elif status == "FAILED":
            raise Exception("Job Failed: %s" % response)
        return False

    def check_notification(self):
        response = self.notification_channel.receive_messages()
        if response:
            if "Messages" not in response:
                return False
        for message in response['Messages']:
            notification = json.loads(message['Body'])
            text_msg = json.loads(notification['Message'])
            if str(text_msg['JobId']) == self.job_id:
                logger.debug('Job Finished: ID=%s' % self.job_id)
                self.notification_channel.delete_message(message['ReceiptHandle'])
                return True
        return False

    def get_results(self):
        token = None
        results = None

        while results is None or token:
            time.sleep(1)
            if token:
                response = self.request_results(JobId=self.job_id, NextToken=token)
            else:
                response = self.request_results(JobId=self.job_id)

            if results is None:
                results = response
            else:
                new_blocks = response['Blocks']
                blocks = results.get("Blocks", [])
                blocks.extend(new_blocks)
                results["Blocks"] = blocks

            token = response.get("NextToken")

        results["NextToken"] = None
        # Save the results to file
        if self.output_uri:
            with StorageFile.init(self.output_uri, "w") as f:
                json.dump(results, f)

        return results
