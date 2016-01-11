import logging
import boto
import boto.s3.connection
import os

class object_storage():
    def __init__(self):
        self.conn= boto.connect_s3(aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                                   aws_secret_access_key=os.environ["S3_SECRET_KEY"],
                                   host=os.environ["S3_HOST"],
                                   port=int(os.environ.get("S3_PORT",443)),
                                   calling_format=boto.s3.connection.OrdinaryCallingFormat())

    def put(self,myBucket,mykey,myString):
        bucket=self.conn.get_bucket(myBucket)
        key = bucket.new_key(mykey)
        key.set_contents_from_string(myString)
        logging.info("Data stored in %s"  % myBucket + "/" + mykey)