#encoding: UTF-8

def load_schema_ontology
  url = 'https://schema.org/version/latest/schemaorg-current-https.jsonld'
  response = RestClient.get(url)
  @schema_org_jsonld = JSON.parse(response)
  File.write("/app/elastic/datamodel/schema_org.json", @schema_org_jsonld)
end

def process_schema_entity(entity:)
  raise "process_entity: entity cannot be nil" if entity.nil?

  known_entities = @datamodel[:_ENTITIES].map { |e| "#{e[:Name]}" }
  return if known_entities.include?(entity)

  schema_entities = @schema_org_jsonld["@graph"].select { |s| s["@id"] == entity }

  if schema_entities.size > 1
    raise "process_entity: schema_entities returns more than 1 entity"
  end

  schema_entity = schema_entities.first
  return unless schema_entity

  @logger.info "Processing schema entity: #{schema_entity["@id"]}"

  result = {
    #Name:        schema_entity["@id"].split(':')[1],
    Name:        schema_entity["@id"],
    NamePlural:  schema_entity["@id"].split(':')[1].pluralize,
    Description: scheme_extract_description(schema_entity),
    #subClassOf:  schema_entity.dig("rdfs:subClassOf", "@id")&.split(':')&.last,
    subClassOf:  schema_entity.dig("rdfs:subClassOf", "@id"),
    sameAs:      schema_entity["@id"]
  }



  @datamodel[:_ENTITIES] << result

  # Recursively process subclass
  if result[:subClassOf]
    process_schema_entity(entity: "#{result[:subClassOf]}")
  end
end

def process_schema_property(property: nil)
  begin
    # prefix = "schema:"
    # property = property.start_with?(prefix) ? property : "#{prefix}#{property}"
    
    @logger.info  "get schema_org property: #{property}"
    property_params = @schema_org_jsonld["@graph"].select { |s| s["@id"] == property }

    unless property_params.size == 1
      if property_params.size == 0
        raise "#{property} not found in schema.org"
      end
      @logger.warn  "property_params: #{property_params}"
      raise "process_schema_property is empty or returns more than 1 property for #{property}"
    end

    property_params =  property_params.first

    #name = property_params["@id"].split(':')[1]
    name = property_params["@id"]

    # pp "-- property_params --"
    # pp property_params
    # pp "---------------------"

    #domainIncludes = [property_params["schema:domainIncludes"]].flatten.select { |s| @types.include?( s["@id"].split(':')[1] ) }
    domainIncludes = [property_params["schema:domainIncludes"]].flatten.select { |s| @types.include?( s["@id"] ) }

    unless domainIncludes.size == 1
      #pp "BUT WHAT IF THE UPPER TYPE IS PART OF IT"
      #pp '"@type" : "OrganizationRole " is not found but   {"@id" => "schema:Role"} is'
      if domainIncludes.size == 0
        @logger.warn  " unfiltered domainIncludes #{  property_params["schema:domainIncludes"] }" 
      end
      @logger.warn  " #{property} is part of multiple domainIncludes #{  domainIncludes }"  
      
      unless domainIncludes.size == 1
        unless [
          "schema:caption",
          "schema:encodingFormat",
          "schema:height",
          "schema:address",
          "schema:startTime",
          "schema:endTime",
          "schema:interactionStatistic",
          "schema:logo",
          "schema:memberOf",
          "schema:keywords",
          "schema:actor",
          "schema:director",
          "schema:duration",
          "schema:productionCompany",
          "schema:musicBy",
          "schema:addressCountry",
          "schema:latitude",
          "schema:longitude",
          "schema:review",
          "schema:provider",
          "schema:wordCount",
          "schema:itemListElement",
          "schema:result"
        ].include?(property)


          raise " #{property} is part of multiple domainIncludes #{ domainIncludes }" 
        end
      end
    end
  
    domainIncludes.each do |entity|
      #entity = entity["@id"].split(':')[1] 
      entity = entity["@id"]
      if @datamodel[entity.to_sym].nil?
        @datamodel[entity.to_sym] = []
      end
        
      datatype = [property_params["schema:rangeIncludes"]]&.flatten.map { |r| r["@id"] }
      datatype = datatype.select { |s| s != "schema:TextObject"}


      # datatype.map! { |m| m.gsub(/^schema:/, "") }
      known_datatypes = ["schema:Text","schema:Date","schema:DateTime","schema:URL","schema:Duration","schema:Distance","schema:Action","schema:Integer"]
      known_datatypes = known_datatypes + @datamodel[:_ENTITIES].map { |m| m[:Name]} 
      
      datatype.select! { |s| known_datatypes.include?(s) } 

      datatype.map! { |d|
        case d      
        when "schema:Text"   
          "xsd:string"  
        when "schema:Date"     
          "xsd:date"  
        when "schema:DateTime"
          "xsd:dateTime"
        when "schema:URL"
          "xsd:anyURI"
        when "schema:Duration"
          "xsd:duration"
        when "schema:Distance"
          "xsd:float"
        when "schema:Integer"
          "xsd:integer"          
        else
          d
        end
      }

      if datatype.empty?
        pp "#{name} is property of #{entity} and can have values of type #{datatype}"
        pp property_params["schema:rangeIncludes"]
        exit
      end
      @logger.info  "#{name} is property of #{entity} and can have values of type #{datatype}"
      already_in_datamodel = @datamodel[entity.to_sym].select { |s| s[:Name] == name}

      if already_in_datamodel.empty?
        @datamodel[entity.to_sym] << {
          Name: name,
          Description: property_params["rdfs:comment"]["@value"]&.gsub(/[\r\n]/, "")&.strip  || property_params["rdfs:comment"]&.gsub(/[\r\n]/, "")&.strip,
          "MIN": nil,
          "MAX": nil,
          sameAs: nil,
          datatype: datatype.join(','),
          Remark: nil
        }
      end
    end
  rescue => e
    
    if property.match?(/schema:[A-Za-z]{2}-[A-Za-z]{4}_.*/)
      @logger.warn  "property \"#{property}\" is an 'admin'property. Not needed in the datamodel !!!"
    elsif 
      property.match?(/schema:china-Hani_alternateName/) ||
      property.match?(/schema:thailand-Thai_alternateName/) ||
      property.match?(/schema:россия-Cyrl_alternateName/) 
    
      @logger.warn  "property \"#{property}\" should not be created at all. Clean this up !!!"
    else
      raise e
    end
  end

end

#
#def get_schema_properties(entity:)
#  return [] if entity.nil?
#  @schema_org_jsonld["@graph"].select do |item|
#    domain_includes = item['schema:domainIncludes']
#    domain_includes = [domain_includes] unless domain_includes.is_a?(Array)
#    domain_includes.any? { |d| d["@id"] == entity }
#  end
#end


def scheme_extract_description(schema_entity)
  comment = schema_entity["rdfs:comment"]
  return "" unless comment

  if comment.is_a?(Hash)
    comment["@value"]&.gsub(/[\r\n]/, "")&.strip
  else
    comment.gsub(/[\r\n]/, "")&.strip
  end
end