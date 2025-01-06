#encoding: UTF-8
$LOAD_PATH << '.' << './lib'
require 'logger'
require_relative './lib/loader'

##########

INDEX1 = "icandid_20240618"
INDEX2 = "icandid_20250101"

pp "mappings_1 INDEX1: #{INDEX1}"
pp "mappings_2 INDEX2: #{INDEX2}"


begin
  

  def check_keys(mappings_1, mappings_2, property)
    begin
#       pp "mapping for #{property}"
      if ( ! mappings_1["properties"].keys.difference( mappings_2["properties"].keys ).empty? )
        pp "for #{property} mappings_1 difference from mappings_2"
        pp mappings_1["properties"].keys.difference( mappings_2["properties"].keys )
      end
  

      if ( ! mappings_2["properties"].keys.difference( mappings_1["properties"].keys ).empty? )
        pp "for #{property} mappings_2 difference from mappings_1"
        pp mappings_2["properties"].keys.difference( mappings_1["properties"].keys )
      end
  
      properties = (mappings_1["properties"].keys + mappings_2["properties"].keys).uniq
  
      properties.each do |f|
        # pp mappings_1["properties"][f]
        if mappings_1["properties"][f] && mappings_2["properties"][f]
          if mappings_1["properties"][f]["properties"] && mappings_2["properties"][f]["properties"] 
    #        pp "properties for #{f}"
    #        pp mappings_1["properties"][f]["properties"].keys
    #        pp mappings_2["properties"][f]["properties"].keys
            check_keys( mappings_1["properties"][f], mappings_2["properties"][f], f )
          end
        end
      end
    end
  end


  #output_dir = "/elastic/import"
  # every docker-container has its own HOSTNAME
  output_dir = "/elastic/import/#{ENV["HOSTNAME"]}"
  output_file = "import.bulk"

  #Create a loader object.
  loader = Loader.new()
  config = loader.config()

  loader.logger.info "use utils for es_cluster : #{loader.es_cluster}"

  loader.check_elastic()
  mappings_1 = loader.get_es_mappings(INDEX1)
  mappings_2 = loader.get_es_mappings(INDEX2)

  check_keys(mappings_1,mappings_2,'top_level')
  
  #pp mappings_1["properties"].keys == mappings_2["properties"].keys
  #pp "mappings_1 difference from mappings_2"
  #pp mappings_1["properties"].keys.difference( mappings_2["properties"].keys ) 

  #pp "#####################################################################################"
  #pp "mappings_2 difference from mappings_1"
  #pp mappings_2["properties"].keys.difference( mappings_1["properties"].keys ) 

  #pp "#####################################################################################"
  #mappings_2["properties"].keys.each do |f|
  #  pp "mappings_2 difference from mappings_1 for field #{f} :"
  #  if mappings_2["properties"][f]["properties"]
  #    pp mappings_2["properties"][f]["properties"].keys
  #    pp mappings_1["properties"][f]["properties"].keys
  #    pp mappings_2["properties"][f]["properties"].keys.difference( mappings_1["properties"][f]["properties"].keys ) 
  #  end
  #end

rescue StandardError => e  # e=>object
 
  # prints the attached string message.
  puts e.message

ensure

  
  pp "--> ENSURE "
  exit


end
























