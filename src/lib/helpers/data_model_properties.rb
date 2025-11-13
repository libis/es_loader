#encoding: UTF-8


Dir.glob(File.join(__dir__, 'ontologies', '*.rb')).each do |file|
  require_relative file
end

def process_properties(properties: {}, parent_prop: nil)
  # Define keys to exclude


  excluded_keys = %w[
    id @context @type prov:type
    dateCreated_time_frame datePublished_time_frame processingTime processingtime
    processingtime_time_frame updatetime create_time processtime start_time
    twitter_author_id count @typ count_test acquiredFrom color
    prov:Agent prov:SoftwareAgent
  ]

  # Filter out unwanted properties
  properties = properties.reject { |k, _| excluded_keys.include?(k) }

  properties.each do |prop_key, prop_value|
    # Skip keys starting with '_' or '@'
    if prop_key.match(/^_|^@/)
      @logger.warn "Skipping property starting with '_' or '@': #{prop_key}"
      next
    end

    current_parent_prop = [parent_prop, prop_key].compact.join('.')

    # Handle nested properties with @type
    if prop_value["properties"]&.key?("@type")
      @logger.info "Processing nested properties for #{prop_key}"
      process_properties(properties: prop_value["properties"], parent_prop: current_parent_prop)

      if prop_key == "prov:wasAssociatedFor"
        # Special case: skip this prov property
        next
      end
    end

    # Handle flat properties
    @logger.info "Getting property description from #{prop_key}"
    @logger.info "Getting types that use property #{prop_key} (parent: #{parent_prop})"

    if m = prop_key.match(/^([^:\s]+):(.*)/)
      prefix, prop_key = m.captures
    else
      prefix, prop_key = @default_prefix, prop_key
    end

    method_name = "process_#{prefix}_property"
    property = send(method_name, property: "#{prefix}:#{prop_key}")
 
  end
end




