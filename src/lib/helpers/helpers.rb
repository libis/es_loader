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

def canonicalize(object)
  case object
  when Array
    # Rule 1: Recursively process and then SORT the array.
    object.map { |item| canonicalize(item) }.sort
  when Hash
    # Rule 2: Recursively process values, then convert to a sorted array of pairs.
    # This maintains deep structure while ignoring key order.
    object.map do |key, value|
      [key, canonicalize(value)]
    end.sort_by { |key, _| key.to_s } # Sort by key to ensure canonical order
  else
    # Rule 3: Base case for numbers, strings, etc.
    object
  end
end

# The final uniq method that uses the canonicalizer
def deep_unordered_uniq(array)
  # Map each original object to its canonical form, then use uniq on the canonical forms.
  # Finally, map back to the original objects.
  
  # 1. Create a Hash mapping canonical form to the original object
  canonical_map = {}
  array.each do |original|
    # Get the canonical form
    canonical_form = canonicalize(original)
    
    # Store the original object, only if the canonical form hasn't been seen yet.
    # If the canonical form IS seen, we skip the current 'original' object.
    canonical_map[canonical_form] ||= original
  end
  
  # 2. Return the unique original objects (the values of the map)
  canonical_map.values
end

# DEPRICATED
# def deep_sort_hash(hash)
#   if hash.is_a?(Array)
#     if hash.size === 1
#       v=hash[0]
#       return v.is_a?(Hash) ||v.is_a?(Array) ? deep_sort_hash(v) : v 
#     end
#     sorted = hash.sort_by do |hash|
#       hash.key?("@id") ? [0, hash["@id"]] : [1, hash["start"]]
#     end
#     return sorted.map { |v| v.is_a?(Hash) ||v.is_a?(Array)  ? deep_sort_hash(v) : v }
#   end
#   sorted = hash.sort.to_h
#   sorted.each do |key, value|
#     if value.is_a?(Hash)
#       sorted[key] = deep_sort_hash(value)
#     elsif value.is_a?(Array)
#       sorted[key] = value.map { |v| v.is_a?(Hash) ||v.is_a?(Array)  ? deep_sort_hash(v) : v }
#     end
#   end
#   sorted
# end

def ensure_array(value)
  value.is_a?(Array) ? value : [value]
end

def merge_generated_data(existing, new_data)
  new_data = ensure_array(new_data)
  existing = ensure_array(existing)
  combined = new_data + existing
  #combined.uniq { |h| deep_sort_hash(h).to_json }
  deep_unordered_uniq(combined)
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
