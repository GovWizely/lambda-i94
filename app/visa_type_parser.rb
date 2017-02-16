require 'roo' 
require 'roo-xls'          
require 'open-uri'
require_relative'decapitalize'

class VisaTypeParser
  extend Decapitalize

  def self.parse(path)
    @path = path
    @spreadsheet = Roo::Spreadsheet.open(@path)
    @spreadsheet.parse(clean: true)

    headers = { country_or_region: "OF RESIDENCE",
                           business: "BUSINESS",
                           pleasure: "PLEASURE",
                           student: "STUDENT",
                        }

    rows = @spreadsheet.sheet(3).parse(headers) # Visa type sheet is at index 3
    filter_rows(rows)

    return build_hash(rows)
  end

  def self.filter_rows(rows)
    rows.delete_at(0) # Remove header row
    rows.reject! { |row| row[:country_or_region].nil? } # Remove nils
  end

  def self.build_hash(rows)
    hash = {}
    rows.each do |row|
      hash[decapitalize(row[:country_or_region])] = {
        business_visa_arrivals: row[:business].to_i,
        pleasure_visa_arrivals: row[:pleasure].to_i,
        student_visa_arrivals: row[:student].to_i
      }
    end
    hash
  end
end