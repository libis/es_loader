#encoding: UTF-8


def get_type_properties(properties: nil, parent: nil, nested: nil)

  properties_with_properties  = properties.select {  |key, properties| properties.has_key?("properties") }

  properties_with_properties.keys.each { |property|
    if properties[property]["type"] == "nested"
      nested = [parent, property].compact.join(".")
    end
    if get_type_properties( properties: properties[property]["properties"], parent: [parent, property].compact.join("."), nested: nested)
      @type_fields << { path: [parent, property, DEFAULT_TYPE_KEY ].compact.join("."), nested: nested}
    end
  }

  if properties.keys.include?("@type")
    return true
  else
    return false
  end
end


def process_all_types
  @types.each do |type|
    @logger.info "Start processing #{type}"
  
    if m = type.match(/^([^:\s]+):(.*)/)
      prefix, type = m.captures
    else
      prefix, type = @default_prefix, type
    end

    method_name = "process_#{prefix}_entity"
    entity = send(method_name, entity: "#{prefix}:#{type}")

  end
  @types = @datamodel[:_ENTITIES].map { |e| e[:Name] }

end