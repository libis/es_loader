#encoding: UTF-8

require 'rest-client'
require 'mustache'

require_relative './helpers/data_model_types'
require_relative './helpers/data_model_properties'
require_relative './helpers/json_path_pruner'
require_relative './loader'
require_relative './config'

DEFAULT_TYPE_KEY = "@type"
DEFAULT_VOCABULARY  = "http://schema.org/"
DEFAULT_PREFIX = "icandid"

VOCABULARIES= {
  "owl"    => "http://www.w3.org/2002/07/owl#",
  "rdfs"   => "http://www.w3.org/2000/01/rdf-schema#",
  "dc"     => "http://purl.org/dc/elements/1.1/",
  "schema" => "http://schema.org/",
  "skos"   => "http://www.w3.org/2004/02/skos/core#",
  "dcterm"   => "http://purl.org/dc/terms/",
  "xsd"    => "http://www.w3.org/2001/XMLSchema#",
  "sh"     => "http://www.w3.org/ns/shacl#",
  "rdf"    => "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  "time"   => "http://www.w3.org/2006/time#"
}

class DataModelBuilder
  attr_reader :datamodel, :types, :config

  def initialize(index: nil, logger:)
    
    @logger = logger
    @log_file = STDOUT

    @index = index
    @datamodel = { _ENTITIES: [], _DATATYPES: [] }
    @type_fields = [ { path: DEFAULT_TYPE_KEY, nested: nil}]
    @types = [DEFAULT_TYPE_KEY]
    @context = {}
    @prefixes = VOCABULARIES.keys
    @used_vocabularies = []
    @default_vocabulary = DEFAULT_VOCABULARY
    @default_prefix = VOCABULARIES.select{|key, value| value == @default_vocabulary}.keys.first
    @known_props_in_datamodel = []

    get_config

    if index.nil?
      @index = @config[:es_index]
      @predefined_properties  = parse_by_extension( @config[:datamodel][:predefined_properties])
      @excluded_properties    = parse_by_extension( @config[:datamodel][:excluded_properties])
      @predefined_types       = parse_by_extension( @config[:datamodel][:predefined_types] )
      @predefined_type_fields = parse_by_extension( @config[:datamodel][:predefined_type_fields])
      
      @datamodel[:_DATATYPES] = @predefined_types
    end


    @default_prefix = "icandid"
  end

  def run
    pp "===> SETUP LOADER"
    setup_loader
    pp "===> SETUP LOADER FOR TYPE AGGREGATIONS (production because elastic mapping does not support aggragetion on @type-fields)"
    setup_prod_loader
    setup_prefixes

    @used_vocabularies.each { |voc_key, voc_url|
      send("load_#{voc_key}_ontology")
      @logger.info "Ontology loaded for #{voc_key}"
    }

    admin_files = ["_METADATA.csv","_README.csv","_REFERENCES.csv"]
    admin_files.each do |filename|
      File.join(@config[:datamodel][:datamodel_config_path], filename)
      File.join(@config[:datamodel][:output_dir], filename)
      FileUtils.cp(File.join(@config[:datamodel][:datamodel_config_path], filename), File.join(@config[:datamodel][:output_dir], filename))
    end

    pp "===> FETCH AND FILTER MAPPINGS from index #{@config[:es_index]}"
    properties = fetch_and_filter_mappings
    
    pp "===> GET FIELD FROM MAPPINGS THAT COULD CONTAIN A TYPE (all fields with @type in the key) from index #{@config[:es_index]}"
    get_type_properties(properties: properties)
    
    #pp Regexp.union(  @predefined_types.keys.map { |s| /#{Regexp.escape(s)}$/ })
    #@type_fields.reject! { |r| Regexp.union(  @predefined_types.keys.map { |s| /#{Regexp.escape(s)}$/ }).match(r[:path]) }

    # pp @type_fields
    pp "===> PREPARE AGGREGATIONS"
    prepare_aggregations

    pp "===> GET TYPE AGGREGATIONS"
    fetch_types_from_aggregations


=begin
    @types = [
      "NewsArticle",
      "ArchiveComponent",
      "AudioObject",
      "Collection",
      "Comment",
      "Conversation",
      "CreativeWork",
      "Dataset",
      "GeoCoordinates",
      "GeoShape",
      "ImageObject",
      "InteractionCounter",
      "Legislation",
      "MediaObject",
      "Message",
      "Movie",
      "Occupation",
      "Organisation",
      "Organization",
      "PerformanceRole",
      "Person",
      "Place",
      "PropertyValue",
      "Review",
      "TextObject",
      "Thing",
      "VideoObject",
      "action",
      "agent",
      "itemListElement",
      "prov:Activity",
      "prov:Agent",
      "Thing"
    ]
=end
    # Create @datamodel[:_ENTITIES] containing all @type/entity-descriptions 
    process_all_types

    @datamodel[:_DATATYPES] = @datamodel[:_DATATYPES].concat @types.map { |type|
      {
        :baseDataTypes => "",
        :Description => "",
        :AllDatatypes => type,
        :EntityRange => "",
        :EntityCount => "",
        :Example => ""
      }
    }

    @logger.info "Process properties of ElasticSearch Mapping"
    # ====> FIRST DO SOMETHING WITH  @predefined_properties
    @predefined_properties.each { |k,v|
      @datamodel[k.to_sym] = v.values
      @known_props_in_datamodel.concat (v.keys) 
    }



    process_properties(properties: properties, parent_prop: nil)
    @logger.info "Export _-admin fields to csv (_DATATYPES,_ENTITIES,_PREFIXES)"
    # export_entities_to_csv
  
    export_metadata_to_csv
    @logger.info "Export datamodel to csv"
    export_datamodel_to_csv
    @logger.info "DATAMODEL CSV FILES ARE CREATED"
  rescue => e
    pp e
    pp "--> RESCUE StandardError"
    puts e.backtrace
  end

  private

  def get_config() 
    command_line_options = get_command_line_options()
    get_system_config()
    # command line options overrule config options 
    if @log_file == "stdout" || @log_file == "STDOUT"
      @log_file = STDOUT
    end

    @logger = Logger.new(@log_file)
    @logger.debug("config_file: #{ @config_file} " )
    @logger.debug("log_file: #{ @log_file} " )
    @logger.debug("command_line_options: #{ command_line_options} " )  
  end


  def get_system_config
    @config_class ||= Config
    @config_class.path        = File.expand_path("../../config", __dir__)
    @config_class.config_file = @config_file

    init_config = @config_class.keys.to_h { |k| [k, @config_class[k]] }

    # Optional shallow dup for common mutable types
    init_config.transform_values! do |v|
      case v
      when String, Array, Hash then v.dup
      else v
      end
    end

    begin
      # Roundtrip only once if you need to allow nested structures via template expansion
      rendered_json = Mustache.render(JSON.generate(init_config), init_config)
      @config = JSON.parse(rendered_json, symbolize_names: true)
    rescue JSON::ParserError, Mustache::Parser::SyntaxError => e
      # Add context then re-raise
      raise e.class, "Config rendering failed: #{e.message}"
    end
  end

  def get_command_line_options
    # @logger.debug("get_command_line_options")
    # Defines the UI for the user. Albeit a simple command-line interface.
    command_line_options = {}
    OptionParser.new do |o|
      o.banner = "Usage: #{$0} [options]"
      o.on("-l LOGFILE", "--log", "write log to file") { |log_file| @log_file = log_file; command_line_options[:log_file] = log_file }
      #Passing in a config file.... Or you can set a bunch of parameters.
      o.on("-c CONFIG", "--config", "Configuration file.") { |config_file| @config_file = config_file; command_line_options[:config_file] = config_file  }
      o.on("-e ESLASTIC_LOGFILE", "--log", "write elastic client log to file") { |client_logger| @client_logger = client_logger; command_line_options[:client_logger] = client_logger  }
      o.on("-d DIRECTORY", "--dir", "Sub directory relative to ./records/ \"dir1,dir2,dir3/dir\"") { |record_dirs_to_load| @record_dirs_to_load = record_dirs_to_load.split(","); command_line_options[:record_dirs_to_load] = record_dirs_to_load.split(",") }
      o.on("-r RECORD_ID_FILE_PATTERN", "--record_id_file_pattern", "filepattern to extract record id. Same id must be preprocessed. If missing no preprocessing will be done") { |record_id_file_pattern| @record_id_file_pattern = record_id_file_pattern; @record_id_file_pattern[:record_id_file_pattern] = record_id_file_pattern  }
      o.on("-p PATTERN", "--pattern", "file pattern of record-filename /.*\\\.json/") { |record_pattern| @record_pattern = record_pattern; command_line_options[:record_pattern] = Regexp.new(record_pattern) }
      o.on("-t LOAD_TYPE", "--load_type", "action: update, reload or reindex") { |load_type| @load_type = load_type; command_line_options[:load_type] = load_type }
      o.on("-u LAST_RUN", "--last_run_updates", "Time the command was last run") { |last_run_updates| @last_run_updates = last_run_updates; command_line_options[:last_run_updates] = last_run_updates }
      #Help screen.
      o.on( '-h', '--help', 'Display this screen.' ) do
        puts o
        exit
      end
      o.parse!   
    end
    command_line_options
  end

  def setup_prefixes
    @config[:datamodel][:vocabularies].each { |k,v|
      VOCABULARIES[k.to_s] = v
    }
    @prefixes = VOCABULARIES.keys

    get_context

    if @context.has_key?("@vocab")
      @default_vocabulary = @context["@vocab"]
    end
    
    @used_vocabularies  = VOCABULARIES.select{|key, value| value == @default_vocabulary}
    @default_prefix = VOCABULARIES.select{|key, value| value == @default_vocabulary}.keys.first
    @prefixes << @default_prefix


    @context.each { |c_k, c_v|
      unless c_k.match(/^@/)
        if c_k.match(/:/)
          pp "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
          pp "TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO"
          pp "Handle this from @context "
          pp "#{c_k} ===>> #{c_v}"
          pp "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        else
          unless VOCABULARIES.keys.include?(c_k)
            VOCABULARIES[c_k.to_s] = c_v
            @logger.warn ("vocabulary prefix \"#{c_k}\" missing in configuration 'VOCABULARY_PREFIXES' ")
          end
          @used_vocabularies[c_k] = VOCABULARIES[c_k]     
          @prefixes << c_k
        end
      end
    }
    
    @prefixes.uniq!.sort!
    @logger.debug("Used prefixes #{@prefixes }")

    @datamodel[:_PREFIXES] = VOCABULARIES.map do |prefix, uri|
      {
        :Base   => "",
        :Prefix => prefix,
        :URI    => uri
      }
    end

  end

  def setup_loader
    @loader = Loader.new
    @loader.config
    @loader.check_elastic
  end

  def fetch_and_filter_mappings
    mappings = @loader.get_es_mappings(@index)
    properties = mappings["properties"]

    JsonPathPruner.prune!( properties, @excluded_properties)

    properties
  end

  #def get_type_properties(properties:)
    # Assuming this is a method defined elsewhere
    # You may need to move or define it inside this class
  #  ::get_type_properties(properties: properties)
  #end

  def prepare_aggregations
   
    # pp Regexp.union(  @predefined_type_fields.keys.map { |s| /^#{Regexp.escape(s)}$/ })
    @type_fields.reject! { |r| Regexp.union(  @predefined_type_fields.keys.map { |s| /^#{Regexp.escape(s)}$/ }).match(r[:path]) }
   
    #pp @type_fields

    @aggs = {}
    @type_fields.each do |type|
      if type[:nested].nil?
        @aggs[type[:path]] = {
          terms: {
            field: type[:path],
            size: 1000
          }
        }
      else
        @aggs[type[:path]] = {
          nested: {
            path: type[:nested]
          },
          aggs: {
            "#{type[:path]}": {
              terms: {
                field: type[:path],
                size: 1000
              }
            }
          }
        }
      end
    end
  end

  def setup_prod_loader
    @prod_loader = Loader.new
    @prod_loader.es_url = @config[:es_url]
    @prod_loader.es_version = @config[:es_version]
    @prod_loader.check_elastic
  end

  def get_context
    hits = @loader.search(
      index: @index,
      body: {
        _source: "@context",
        size: 1,
        query: { match_all: {} }
      }
    )
    @context = hits["hits"]["hits"].first["_source"]["@context"]

    if @context.is_a?(Array)
      @context.reject!{ |e| e == {"schema" => "schema.org"} }
      @context = @context.first
    end
    @context

  end

  def fetch_types_from_aggregations
    aggregations = @prod_loader.search(
      index: @index,
      body: {
        size: 0,
        track_total_hits: true,
        query: { match_all: {} },
        aggs: @aggs
      }
    )

    types = aggregations["aggregations"].flat_map do |agg_key, agg_value|
      if agg_value.has_key?(agg_key) # Nested Aggregations
        agg_value[agg_key]["buckets"].map { |b| b["key"] }
      else
        agg_value["buckets"].map { |b| b["key"] }
      end
      
    end

    types = types.concat( @predefined_type_fields.values.flatten )
    @types = types.compact.uniq.sort

    @types

  end

  def export_entities_to_csv
    csv_column_names = @datamodel[:_ENTITIES].map(&:keys).max_by(&:size)
    props = @datamodel[:_ENTITIES]

    props.map! { |p| p[:Name] = p[:Name].gsub( /^#{@default_prefix}:/, '' ) ; p}

    csv_data = CSV.generate do |csv|
      csv << csv_column_names
      props.each { |x| csv << x.values }
    end

    File.open(File.join( @config[:datamodel][:output_dir], "_ENTITIES.csv"), "w") do |file|
      file.write(csv_data)
    end
    @datamodel.delete(:_ENTITIES)
  end

  def export_metadata_to_csv
    metadata_fields = [:_DATATYPES,:_ENTITIES,:_PREFIXES]
    
    @datamodel[:_PREFIXES].map! { |p| 
      if p[:Prefix] == DEFAULT_PREFIX
        p[:Base] = '*'
      end
      p
    }

    metadata_fields.each do |field|
      csv_column_names = @datamodel[field].map(&:keys).max_by(&:size)
      # pp "csv_column_names : #{csv_column_names}"
      props = @datamodel[field]

      props.map! { |p| 
        if p.has_key?( :Name )
          p[:Name] = p[:Name].gsub( /^#{@default_prefix}:/, '' ) 
        end
        p
      }

      csv_data = CSV.generate do |csv|
        csv << csv_column_names
        props.each { |x| csv << x.values }
      end
      pp "--------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
      File.open(File.join( @config[:datamodel][:output_dir], "#{field.to_s}.csv"), "w") do |file|
        file.write(csv_data)
      end
      @datamodel.delete(field)
    end
  end
  

  def export_datamodel_to_csv
    @datamodel.each do |entity, props|
      csv_column_names = props.map(&:keys).max_by(&:size)

      entity_name = entity.to_s.gsub( /^#{@default_prefix}:/, '' ).gsub(/^:/, "").gsub(/:/, "_")

      props.map! { |x| 
        x[:Name] = x[:Name].to_s.gsub( /^#{@default_prefix}:/, '' )
        x
      }

      csv_data = CSV.generate do |csv|
        csv << csv_column_names
        props.each { |x| csv << x.values }
      end

      File.open( File.join( @config[:datamodel][:output_dir], "#{entity_name}.csv") , "w") do |file|
        file.write(csv_data)
      end
    end
  end
end