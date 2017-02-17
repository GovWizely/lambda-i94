#!/usr/bin/env ruby
require 'aws-sdk-core'
require_relative 'data_builder'

json = DataBuilder.new(bucket_name: 'i94').run
s3 = Aws::S3::Client.new
response = s3.put_object(acl: "public-read", bucket: "i94-json", key: "i94-entries.json", body: json)


