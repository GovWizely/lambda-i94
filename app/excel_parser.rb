require 'roo' 
require 'roo-xls'          
require 'open-uri'
require 'yaml'
require_relative 'decapitalize'
require_relative 'region_parser'
require_relative 'constants'

class ExcelParser
  extend Decapitalize
  extend Constants

  REGIONS = ['Western Europe', 'Eastern Europe', 'Asia', 'Middle East', 'Africa',
    'Oceania', 'South America', 'Central America', 'Caribbean']

  def self.parse(path, region_dictionary)
    @path = path
    @spreadsheet = Roo::Spreadsheet.open(@path)
    @spreadsheet.parse(clean: true)
    @region_dictionary = region_dictionary
    @ntto_groups = build_ntto_groups

    headers = { country: /Country of Residence/,
                "Jan" => /Jan/,
                "Feb" => /Feb/,
                "Mar" => /Mar/,
                "Apr" => /Apr/,
                "May" => /May/,
                "Jun" => /Jun/,
                "Jul" => /Jul/,
                "Aug" => /Aug/,
                "Sep" => /Sep/,
                "Oct" => /Oct/,
                "Nov" => /Nov/,
                "Dec" => /Dec/ }

    rows = @spreadsheet.parse(headers)
    rows = filter_rows(rows)
    return transform_rows(rows)
  end

  def self.transform_rows(rows)
    year = @path.match(/[0-9]{4}/)[0].to_i
    new_rows = []
    rows.each do |row|
      country_or_region = row.delete(:country)

      row.each do |k, v|
        date = Date.new(year, Date::ABBR_MONTHNAMES.index(k), 1) 
        date_str = date.strftime("%Y-%m")

        hash = parse_country_or_region(country_or_region, year)
        hash = hash.merge({ date: date_str, total_arrivals: v.to_i }) unless v.nil?
        add_ntto_groups(hash)

        new_rows.push(hash) unless v.nil?
      end
    end
    new_rows
  end

  def self.filter_rows(rows)
    rows.reject! { |row| row[:country].nil? } # Remove nils first
    # Remove invalid data:  (everything below MEXICO)
    split_index = rows.index{|row| row[:country].match(/MEXICO/)}
    rows.slice!(split_index..rows.size-1) unless split_index.nil?
    rows.reject! { |row| row[:country].match(/INVALID/) } # Catch any Invalids that slipped through
    rows.delete_at(0) # Remove headers row
    rows
  end

  def self.parse_country_or_region(country_or_region, year)
    country_or_region = decapitalize(country_or_region)
    if REGIONS.include?(country_or_region)
      regions = [country_or_region]
    else
      regions = @region_dictionary.key?(country_or_region) ? [@region_dictionary[country_or_region][year.to_s]] : []
    end
    { i94_country_or_region: country_or_region, ntto_groups: regions }
  end

  def self.add_ntto_groups(hash)
    if @ntto_groups[:visa_waiver].include?(hash[:i94_country_or_region])
      hash[:ntto_groups].push 'Visa Waiver'
    elsif !REGIONS.include?(hash[:i94_country_or_region])
      hash[:ntto_groups].push 'Non-Visa Waiver'
    end
    hash[:ntto_groups].push('APEC') if @ntto_groups[:apec].include?(hash[:i94_country_or_region])
    hash[:ntto_groups].push('EU') if @ntto_groups[:eu].include?(hash[:i94_country_or_region])
    hash[:ntto_groups].push('OECD') if @ntto_groups[:oecd].include?(hash[:i94_country_or_region])
    hash[:ntto_groups].push('PATA') if @ntto_groups[:pata].include?(hash[:i94_country_or_region])
    hash[:ntto_groups].push 'Overseas'
    hash[:ntto_groups].compact!                                                                 
  end

  def self.build_ntto_groups
    ntto_groups = {}
    ntto_groups[:visa_waiver] = Constants::VISA_WAIVER_COUNTRIES
    ntto_groups[:apec] = Constants::APEC_COUNTRIES
    ntto_groups[:eu] = Constants::EU_COUNTRIES
    ntto_groups[:oecd] = Constants::OECD_COUNTRIES
    ntto_groups[:pata] = Constants::PATA_COUNTRIES
    ntto_groups
  end
end