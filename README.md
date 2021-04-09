# midPoint-s3-csv-connector

This connector outputs CSV to Cloud Object Storage. It matches the functionality 
 of the existing CSV connector and only differs on where and how the CSV file
  is exported.  

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