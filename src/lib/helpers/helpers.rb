#encoding: UTF-8 

class Array
  def uniqBeginString
    self.uniq!
    array = self
    if array.map { |string| string.class }.uniq === [String]
      array = self.select { |str|
        output = true
        self.each { |el|
          if str != el
            if el.start_with?(str)
              el.start_with?(str)
              output = false
            end
          end
        } 
        output
      }
    end
    array
  end
end

def deep_sort_hash(hash)
  sorted = hash.sort.to_h
  sorted.each do |key, value|
    if value.is_a?(Hash)
      sorted[key] = deep_sort_hash(value)
    elsif value.is_a?(Array)
      sorted[key] = value.map { |v| v.is_a?(Hash) ? deep_sort_hash(v) : v }
    end
  end
  sorted
end

def ensure_array(value)
  value.is_a?(Array) ? value : [value]
end

def merge_generated_data(existing, new_data)
  new_data = ensure_array(new_data)
  existing = ensure_array(existing)
  combined = new_data + existing
  combined.uniq { |h| deep_sort_hash(h).to_json }
end

def xml_to_hash(data, options = {})
  # gsub('&lt;\/', '&lt; /') outherwise wrong XML-parsing (see records lirias1729192 )
  return unless data.is_a?(String)
  data.force_encoding('UTF-8')
  data = data.encode("UTF-8", invalid: :replace, replace: "")
  data = data.gsub /&lt;/, '&lt; /'

  xml_typecast = options.with_indifferent_access.key?('xml_typecast') ? options.with_indifferent_access['xml_typecast'] : true
  nori = Nori.new(parser: :nokogiri, advanced_typecasting: xml_typecast, strip_namespaces: true, convert_tags_to: lambda { |tag| tag.gsub(/^@/, '_') })
  nori.parse(data)
end
