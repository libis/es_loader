#encoding: UTF-8

def load_prov_ontology
  response = RestClient.get("https://www.w3.org/ns/prov.owl", accept: :xml, content_type: :xml)
  @prov_jsonld = xml_to_hash(response)
end

def process_prov_entity(entity: nil)

  if entity.nil?
    raise "process_entity: entity can not be nil"
  end

  known_entities = @datamodel[:_ENTITIES].map { |e| "#{e[:Name]}" }

  if known_entities.include?(entity)
    return
  end
  
  prov_entities = JsonPath.on(@prov_jsonld['RDF'], '*[?(@.label == '+ entity.gsub(/^prov:/, '') +')]')
  if prov_entities.nil?
    pp "==========================>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    prov_entities = JsonPath.on(@prov_jsonld['RDF'], 'ObjectProperty[?(@.label == '+ (entity.gsub(/^prov:/, '').downcase) +')]')
  end

  prov_entities = prov_entities.uniq { |hash| Marshal.dump(hash) }

  
  if prov_entities.size > 1
    if entity == "prov:SoftwareAgent"
      prov_entities =  prov_entities.select{ |s| s["subClassOf"]["_rdf:resource"] == "http://www.w3.org/ns/prov#Agent" }
    else
      pp prov_entities
      prov_entity =  prov_entities.first
      pp prov_entity["subClassOf"]
      raise "process_entity: prov_entities returns more than 1 entity"
    end
  end

  prov_entity =  prov_entities.first
  unless prov_entity["subClassOf"].nil?
    subClassOf = prov_entity["subClassOf"]["_rdf:resource"]
    subClassOf = subClassOf.gsub(/http:\/\/www.w3.org\/ns\/prov#/,'prov:')
  end

  description_fields = [ "definition", "editorsDefinition" , "comment"]

  prov_entity["Description"] = description_fields.map do |df|
    if prov_entity.has_key?(df)
      if prov_entity[df].is_a?(String)
        ret = prov_entity[df]
      end
      if prov_entity[df].is_a?(Hash)
        if prov_entity[df].has_key?("$text")
          ret = prov_entity[df]["$text"]
        end
      end
    end
    ret
  end.compact.join('; ')

  result = {}
  result[:Name]        = "prov:#{ prov_entity["label"] }"
  result[:NamePlural]  = "prov:#{prov_entity["label"].pluralize}", 
  result[:Description] = prov_entity["Description"]["@value"]&.gsub(/[\r\n]/, "")&.strip  || prov_entity["Description"]&.gsub(/[\r\n]/, "")&.strip
  result[:subClassOf]  = subClassOf
  result[:sameAs]      = ""

  @datamodel[:_ENTITIES] << result
 
end

def process_prov_property(property: nil)
  prefix = ""
  property = property.start_with?(prefix) ? property : "#{prefix}#{property}"
  
  @logger.info  "get prov property: #{property}"

  property_params = JsonPath.on(@prov_jsonld['RDF'], '*[?(@.label == '+ property.gsub(/^prov:/, '') +')]')
  if property_params.nil?
    pp "==========================>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    property_params = JsonPath.on(@prov_jsonld['RDF'], 'ObjectProperty[?(@.label == '+ (property.gsub(/^prov:/, '').downcase) +')]')
  end

  property_params = property_params.select{ |s| ! (s["range"].nil? || s["domain"].nil?) } 
  property_params = property_params.uniq { |hash| Marshal.dump(hash) }

  if property_params.size > 1
    raise "property_params: property_params returns more than 1 entity"
  end


  property_params = property_params.first

  #pp "-- property_params --"
  #pp property_params
  #pp "---------------------"

  description_fields = [ "definition", "editorsDefinition" , "comment" ]

  property_params["Description"] = description_fields.map do |df|
    if property_params.has_key?(df)
      if property_params[df].is_a?(String)
        ret = property_params[df]
      end
      if property_params[df].is_a?(Hash)
        if property_params[df].has_key?("$text")
          ret = property_params[df]["$text"]
        end
      end
    end
    ret
  end.compact.join('; ')

  unless property_params["range"].nil?
    datatype = property_params["range"]["_rdf:resource"].gsub(/http:\/\/www.w3.org\/ns\/prov#/,'prov:') 
    datatype = datatype.gsub(/http:\/\/www.w3.org\/2001\/XMLSchema#(\w+)/) { |match| "#{$1[0].upcase}#{$1[1..]}" }
  end
  unless property_params["domain"].nil?
    entity = property_params["domain"]["_rdf:resource"].gsub(/http:\/\/www.w3.org\/ns\/prov#/,'prov:')
  end
  
  #if entity == "prov:Entity"
  #  entity = "schema.org:CreativeWork"
  #end
  #if datatype == "prov:Entity"
  #  datatype = "schema.org:Thing"
  #end

  name = "prov:#{ property_params["label"] }"

  @logger.info  "#{name} is property of #{entity} and can have values of type #{datatype}"
  if entity.nil?
    raise "no entity" 
  end

  if @datamodel[entity.to_sym].nil?
    @datamodel[entity.to_sym] = []
  end
  already_in_datamodel = @datamodel[entity.to_sym].select { |s| s[:name] == name}

  if already_in_datamodel.empty?
    @datamodel[entity.to_sym] << {
      Name: name,
      Description: property_params["Description"] .gsub('\n',''),
      "MIN": nil,
      "MAX": nil,
      sameAs: "prov:#{ property_params["label"] }",
      datatype: datatype, # .join(','),
      Remark: nil
    }
  else
    @logger.info  "already_in_datamodel: #{name}"
    pp @datamodel
    pp "already_in_datamodel #{already_in_datamodel}"
    exit
  end
end