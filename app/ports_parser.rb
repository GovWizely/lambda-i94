require 'roo' 
require 'roo-xls'          
require 'open-uri'
require_relative 'decapitalize'

class PortsParser
  extend Decapitalize

  NEW_PORTS_SHEETS = [15, 19, 23, 27, 31]
  NEW_HEADERS_ROW = 8
  OLD_PORTS_SHEETS = [13, 17, 21, 25]
  OLD_HEADERS_ROW = 7

  def self.parse(path)
    set_instance_vars(path)
    return_hash = {}

    @sheet_indices.each do |sheet_index|
      headers = @spreadsheet.sheet(sheet_index).row(@headers_index)
      rows = @spreadsheet.sheet(sheet_index).parse(header_search: headers)
      filter_rows(rows)
      return_hash = transform_rows(rows, sheet_index, return_hash)
    end

    return_hash
  end

  def self.transform_rows(rows, sheet_index, return_hash)
    rows.each do |row|
      port = decapitalize(row.delete("PORTS"))
      row.each.map do |k, v|
        if (return_hash.key?(decapitalize(k)) && port != "Grand Total") # Don't need to include the total here
          return_hash[decapitalize(k)][:ports_arrivals].push({ port: port, amount: v.to_i })
        elsif port != "Grand Total"
          return_hash[decapitalize(k)] = { ports_arrivals: [{ port: port, amount: v.to_i }] }
        end
      end
    end

    return_hash
  end

  def self.filter_rows(rows)
    rows.delete_at(0)
    rows.reject! { |row| row["PORTS"].nil? } # Remove nils
  end

  def self.set_instance_vars(path)
    @spreadsheet = Roo::Spreadsheet.open(path)
    @spreadsheet.parse(clean: true)
    year = path.match(/[0-9]{4}/)[0].to_i
    if year >= 2014
      @sheet_indices = NEW_PORTS_SHEETS
      @headers_index = NEW_HEADERS_ROW
    else
      @sheet_indices = OLD_PORTS_SHEETS
      @headers_index = OLD_HEADERS_ROW
    end
  end
end