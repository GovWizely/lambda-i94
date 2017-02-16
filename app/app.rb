#!/usr/bin/env ruby
require 'aws-sdk-core'
require_relative 'data_builder'

DataBuilder.new(bucket_name: 'i94')

puts "event: #{ARGV[0]}"
puts "context: #{ARGV[1]}"
