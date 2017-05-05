require 'aws-sdk-core'
require_relative 'excel_parser'
require_relative 'canada_mexico_parser'
require_relative 'visa_type_parser'
require_relative 'ports_parser'
require_relative 'taxonomy_mapper'

class DataBuilder

  attr_accessor :visa_type_dictionary, :ports_dictionary

  def initialize(args)
    @s3 = args[:s3] ? args[:s3] : Aws::S3::Client.new
    @bucket_name = args[:bucket_name]
    @visa_type_dictionary = {}
    @ports_dictionary = {}
    @taxonomy_mapper = TaxonomyMapper.new
  end

  def run
    file_paths = @s3.list_objects(bucket: "#{@bucket_name}").contents.map{ |f| f.key }

    @s3.get_object(
      response_target: '/tmp/regions.xls',
      bucket: @bucket_name,
      key: 'regions.xls')
    region_dictionary = RegionParser.parse('/tmp/regions.xls')

    build_additional_amounts(file_paths)
    root_data = build_root_data(file_paths, region_dictionary)
    data = add_additional_amounts(root_data)
    add_taxonomy_fields(data)

    JSON.pretty_generate(data)
  end

  def add_taxonomy_fields(data)
    data.map do |entry|
      @taxonomy_mapper.add_taxonomy_fields(entry)
    end
  end

  def add_additional_amounts(root_data)
    root_data.each do |entry|
      if @visa_type_dictionary.key?(entry[:date]) && @visa_type_dictionary[entry[:date]].key?(entry[:i94_country_or_region])
        entry.merge!(@visa_type_dictionary[entry[:date]][entry[:i94_country_or_region]])
      else
        entry.merge!({business_visa_arrivals: "", pleasure_visa_arrivals: "", student_visa_arrivals: ""})
      end

      if @ports_dictionary.key?(entry[:date]) && @ports_dictionary[entry[:date]].key?(entry[:i94_country_or_region])
        entry.merge!(@ports_dictionary[entry[:date]][entry[:i94_country_or_region]])
      else
        entry.merge!(ports_arrivals: [])
      end
    end

    root_data
  end

  def build_root_data(file_paths, region_dictionary)
    data = []
    file_paths.each do |path|
      next if path == 'regions.xls' || path.include?('visa_types_ports')

      temp_path = "/tmp/#{path}"
      @s3.get_object(
        response_target: temp_path,
        bucket: @bucket_name,
        key: path)
      if path.include?("canada_mexico.xlsx")
        data.concat CanadaMexicoParser.parse(temp_path)
      else
        data.concat ExcelParser.parse(temp_path, region_dictionary)
      end
    end
    data
  end

  def build_additional_amounts(paths)
    paths.each do |path|
      if path.include?('visa_types_ports')
        temp_path = "/tmp/#{path}"
        @s3.get_object(
          response_target: temp_path,
          bucket: @bucket_name,
          key: path)
        year = path.match(/[0-9]{4}/)[0]
        month = path.match(/[A-Z][a-z]{2}.xls/)[0].sub!('.xls', '')

        date = Date.new(year.to_i, Date::ABBR_MONTHNAMES.index(month), 1) 
        date_str = date.strftime("%Y-%m")

        @visa_type_dictionary[date_str] = VisaTypeParser.parse(temp_path)
        @ports_dictionary[date_str] = PortsParser.parse(temp_path)
      end
    end
  end

  def write_json_file(data)
    File.open('i94.json', 'w'){|f| f.write(JSON.pretty_generate(data))}
  end
end