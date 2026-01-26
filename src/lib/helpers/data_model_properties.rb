#encoding: UTF-8


Dir.glob(File.join(__dir__, 'ontologies', '*.rb')).each do |file|
  require_relative file
end

def process_properties(properties: {}, parent_prop: nil)
  # Define keys to exclude
=begin (deprecated see excluded_properties)  
  excluded_keys = %w[
    id @context @type prov:type
    dateCreated_time_frame datePublished_time_frame processingTime processingtime
    processingtime_time_frame updatetime create_time processtime start_time
    twitter_author_id count @typ count_test acquiredFrom color
    prov:Agent prov:SoftwareAgent
  ]

  # Filter out unwanted properties
  properties = properties.reject { |k, _| excluded_keys.include?(k) }
=end


  begin

    properties.each do |prop_key, prop_value|
      # Skip keys starting with '_' or '@'
      if prop_key.match(/^_|^@/)
        @logger.warn "Skipping property starting with '_' or '@': #{prop_key} ???????????????????????!!!!!!!"
        next
      end

      current_parent_prop = [parent_prop, prop_key].compact.join('.')

      # Handle nested properties with @type
      if prop_value["properties"]&.key?("@type")
        # Special case: 

        @logger.info "Processing nested properties for #{prop_key}"
        process_properties(properties: prop_value["properties"], parent_prop: current_parent_prop)

        pp "===> hier reverse of verwerken !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

        pp @context

        if prop_key == "prov:wasAssociatedFor"
          pp @datamodel.keys
=begin
          @datamodel["schema:Thing".to_sym] << {
            Name: "prov:wasAssociatedFor",
            Description: "is inverse of prov:wasAssociatedWith",
            "MIN": nil,
            "MAX": nil,
            sameAs: "prov:wasAssociatedFor",
            datatype: "prov:Agent,schema:agent",
            Remark: ""
          }
=end
          prop_key = "prov:wasAssociatedWith"

        end
      end

      # Handle flat properties
      if m = prop_key.match(/^([^:\s]+):(.*)/)
        prefix, prop_key = m.captures
      else
        prefix, prop_key = @default_prefix, prop_key
      end

      @logger.info "Process property #{prop_key} with prefix #{prefix}"

      unless @known_props_in_datamodel.include?("#{prefix}:#{prop_key}")
        if @default_prefix == prefix
          unless @known_props_in_datamodel.include?("#{prop_key}")
            method_name = "process_#{prefix}_property"
            property = send(method_name, property: "#{prefix}:#{prop_key}", mapping: prop_value)
          end
        end
      end

      # if prop_value["properties"]&.key?("@type")
      #   pp "@type"
      # end
      # if prop_key == "inLanguage"
      #   pp "parent_propparent_propparent_prop #{parent_prop}"
      #   pp  prop_value
      #   pp "parent_propparent_propparent_prop #{parent_prop}"
      #   pp property
      #   exit
      # end

      @known_props_in_datamodel << "#{prefix}:#{prop_key}"

    end
  rescue StandardError => e 
      pp e
      exit
  end
end




