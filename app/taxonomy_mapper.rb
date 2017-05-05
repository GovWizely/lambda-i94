require 'open-uri'

class TaxonomyMapper
  MAPPER_TEMPLATE = "http://im.govwizely.com/api/terms.json?mapped_term=%s&source=I94MonthlyData&log_failed=true"
  TAXONOMY_TEMPLATE = "https://api.trade.gov/ita_taxonomies/search.json?size=1&types=Countries&api_key=%s&q=%s"
  MAPPER_CONFIG = [{starting_field: :i94_country_or_region, desired_field: :country}, {starting_field: :i94_country_or_region, desired_field: :world_region}]

  def initialize
    @cache = {}
  end

  def add_taxonomy_fields(entry)
    MAPPER_CONFIG.each do |config|
      term = nil
      entry[config[:desired_field]] = ''
      url = URI.escape(MAPPER_TEMPLATE % entry[config[:starting_field]])
      mapper_response = JSON.parse(cached_response_for(url))
      term = mapper_response.first unless mapper_response.empty?
      if country_should_be_added?(term, config)
        entry[config[:desired_field]] = term["name"]
      elsif world_regions_should_be_added?(term, config)
        entry[config[:desired_field]] = add_world_region(term, mapper_response)
      end
    end
    entry
  end

  private

  def add_world_region(term, mapper_response)
    if term["taxonomies"].include?("Countries")
      country = term["name"]
      url = URI.escape(TAXONOMY_TEMPLATE % [ENV['API_KEY'], country])
      taxonomy_response = JSON.parse(cached_response_for(url))
      return taxonomy_response["results"].first["related_terms"]["world_regions"]
    elsif term["taxonomies"].include?("World Regions")
      return mapper_response.map{ |term| term["name"] }
    else
      return ""
    end
  end

  def country_should_be_added?(term, config)
    term && config[:desired_field] == :country && term["taxonomies"].include?("Countries")
  end

  def world_regions_should_be_added?(term, config)
    term && config[:desired_field] == :world_region
  end

  def cached_response_for(url)
    if @cache.key?(url)
      @cache[url]
    else
      response = open(url, {ssl_verify_mode: 0}).read
      @cache[url] = response
      response
    end
  end
end