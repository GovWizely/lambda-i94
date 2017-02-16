module Decapitalize
  ABBREVIATIONS = %w(USSR PRC GU VI PR AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY)

  def decapitalize(country_or_region)
    country_or_region_dup = country_or_region.dup
    country_or_region.scan(/([A-Z]+)/).each do |word|
      word = word.first

      next if ABBREVIATIONS.include?(word)

      new_word = word.downcase
      new_word = new_word.capitalize unless ( new_word == "of" || new_word == "and" )

      country_or_region_dup.sub!(word, new_word)
    end

    return country_or_region_dup
  end
end