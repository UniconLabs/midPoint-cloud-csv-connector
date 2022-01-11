# midPoint-cloud-csv-connector

This connector outputs CSV to Cloud Object Storage. It matches the functionality 
 of the existing CSV connector and only differs on where and how the CSV file
  is exported.  

Created by Unicon Inc. for the University of Wisconsin Madison, using the CSV Connector created by Evolveum.   

Currently, it supports these cloud services only:
- AWS S3

TODO Future: Azure Blob Storage? Google Cloud Storage? Linode?


It's strongly recommended to add timeouts to your midPoint resource!

```xml
        <icfc:timeouts>
            <icfc:create>180000</icfc:create>
            <icfc:get>180000</icfc:get>
            <icfc:update>180000</icfc:update>
            <icfc:delete>180000</icfc:delete>
            <icfc:test>60000</icfc:test>
            <icfc:scriptOnConnector>180000</icfc:scriptOnConnector>
            <icfc:scriptOnResource>180000</icfc:scriptOnResource>
            <icfc:authentication>60000</icfc:authentication>
            <icfc:search>180000</icfc:search>
            <icfc:validate>180000</icfc:validate>
            <icfc:sync>180000</icfc:sync>
            <icfc:schema>60000</icfc:schema>
        </icfc:timeouts>
```

## Unit Tests
To run the unit tests it is needed to configure the server/computer where the tests are going to run to have access to AWS. 
If the server/computer is not configured with IAM to have access to S3 (preferred), 
then usually can be achieved with a file called config or credentials in the `~.aws` folder that contains the `aws_access_key_id` and the 
`aws_secret_access_key` and optionally, the `region`. 

There is needed to define the bucket name for the tests (it must be created in advance in the AWS account)
and that must be done in a file called `app-test.properties` in the `src/test/resources` folder, like this:

```
awstest.bucket = yourtestbucketnameforthes3connector
awstest.region = us-west-2
```
If there is not a way to configure the access to AWS and an specific test bucket, the unit tests must be skipped.
