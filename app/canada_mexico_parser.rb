require 'roo' 
require 'roo-xls'          
require 'open-uri'

class CanadaMexicoParser

  def self.parse(path)
    @spreadsheet = Roo::Spreadsheet.open(path)
    @spreadsheet.parse(clean: true)

    headers = {}  #Get valid headers (years) from 4th row:
    @spreadsheet.row(4).each_with_index{ |header, i| headers[header] = i unless header.nil? }
    
    data = []
    data.concat(transform_rows(headers, 'Canada', 574))
    data.concat(transform_rows(headers, 'Mexico', 582))
    data.concat(transform_rows(headers, 'Overseas', 0))
    data.concat(transform_rows(headers, 'International', 0))

    return data
  end

  def self.transform_rows(headers, country_or_region, code)
    transformed_rows = []
    # Iterate over rows we need:
    (5..19).each do |row_num|
      # Only look at row if it starts with a month:
      if Date::MONTHNAMES.include?(@spreadsheet.sheet(country_or_region).row(row_num)[0])
        month = @spreadsheet.sheet(country_or_region).row(row_num)[0]
        # Retrieve amount for each year across row:
        headers.each do |k, v|
          date = Date.new(k.to_i, Date::MONTHNAMES.index(month), 1)
          date_str = date.strftime("%Y-%m")
          amount = @spreadsheet.sheet(country_or_region).row(row_num)[v]

          hash = parse_country_or_region(country_or_region)

          transformed_rows.push(hash.merge({ date: date_str, total_arrivals: amount.to_i })) unless amount.nil?
        end
      end
    end

    return transformed_rows
  end

  def self.parse_country_or_region(country_or_region)
    if ['Overseas', 'International'].include?(country_or_region)
      region = [country_or_region]
    else
      region = ['North America', 'Non-Visa Waiver', 
        'APEC', 
        'OECD']
    end
    { i94_country_or_region: country_or_region, ntto_groups: region }
  end
end