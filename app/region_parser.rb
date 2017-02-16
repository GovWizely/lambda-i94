require 'roo' 
require 'roo-xls'          
require 'open-uri'
require_relative 'decapitalize'

class RegionParser
  extend Decapitalize

  REGIONS = ['Western Europe', 'Eastern Europe', 'Asia', 'Middle East', 'Africa',
    'Oceania', 'South America', 'Central America', 'Caribbean']

  def self.parse(path)
    @spreadsheet = Roo::Spreadsheet.open(path)
    @spreadsheet.parse(clean: true)

    data = {}

    @spreadsheet.sheets.each do |sheet|
      year = /[0-9]{4}/.match(sheet)[0]
      
      start_index = 2 # Skip first 2 rows
      end_index = @spreadsheet.sheet(sheet).column(1).size-4 # Ignore last 3 rows

      entries = @spreadsheet.sheet(sheet).column(1).slice(start_index..end_index)
      build_dictionary(entries, data, year)
    end

    return data
  end

  def self.build_dictionary(entries, data, year)
      region = ""
      entries.each do |entry|
        entry = decapitalize(entry)
        if REGIONS.include?(entry)
          region = entry
        else
          data[entry] = {} unless data.key?(entry)
          data[entry][year] = region
        end
      end
  end
end