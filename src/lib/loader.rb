
class Loader

  require 'net/smtp'
  require 'logger'
  require 'fileutils'
  require 'json'
  require 'yaml' #Used for configuration files.
  require 'pp'
  require 'date'
  require 'base64' #Needed if managing encrypted passwords.
  require 'time'
  require 'optparse'
  require 'elasticsearch'
  require 'data_collector'

  include DataCollector::Core

  #Common classes
  #require_relative './reires_utils'
  #require_relative './icandid_utils'
  require_relative './es_index'
  require_relative './config'
  require_relative './helpers/helpers'
  require_relative './helpers/process_enrichment'

  MAX_RESULTS_LIMIT = 300 #Number of records per file 

  attr_accessor :config, 
                :config_file, 
                :log_file, #log file.
                :logger,

                :last_run_updates, 

                :max_records_per_file,

                :direct_load,

                :record_dirs_to_load,
                :record_pattern, 

                :es_url,
                :es_version,
                :es_index,
                :es_cluster,
                :es_pipeline_id,

                :import_mappings, 
                :import_settings,
                :import_pipeline,

                :audit,

                :total_nr_of_bulk_files,
                :total_nr_of_processed_files
  
  def initialize()

    Encoding.default_external = "UTF-8"

    #@log_file             = './logs/es_loader.log'
    @log_file             = STDOUT
    @log_level            = Logger::DEBUG    
    @logger               = Logger.new(@log_file)

    @log_es_client        = false

    @client_logger        = Logger.new("#{File.dirname(__FILE__)}/../../logs/es_client.log")

    @config_file          = "config.yml"

    @command_line_options = {}
    @last_run_updates     = DateTime.now.to_s

    @direct_load          = false

    @max_records_per_file = MAX_RESULTS_LIMIT
    @load_type            = "update"   # possible values: update, reload, reindex
    #    @mandatory_config     = [@record_dirs_to_load,@record_pattern,@es_url,@es_version,@es_index,@es_pipeline_id,@es_cluster] 

    @record_id_file_pattern = nil
    @mandatory_config     = []

    @record_dirs_to_load  = []

    @record_pattern       = /.*\.json/

    @temp_output_dir      = "/elastic/import/#{ENV["HOSTNAME"]}"
    @temp_output_file     = "import.bulk"

    @jsonoutput           = []

    @rule_set             = nil

    @es_version           = nil
    @es_url               = nil
    @es_cluster           = nil
    @es_index             = nil
    @es_pipeline_id       = nil

    @es_client            = nil
    @import_mappings      = "/elastic/mappings.json"
    @import_settings      = "/elastic/settings .json"
    @import_pipeline      = "/elastic/pipeline.json"

    @audit                = {:mailfrom => "loader@example.com", :mailto => "support@example.com"}

  end

  #get configuration: default settings, command-line options, yml config-file.
  def config() 
    get_command_line_options()
    get_system_config()
    # command line options overrule config options 
    @command_line_options.each{ |o,v|
      instance_variable_set("@#{o}", v)
    }
    if @log_file == "stdout" || @log_file == "STDOUT"
      @log_file = STDOUT
    end

    # @logger = Logger.new(@log_file)
    @logger.debug("config_file: #{ @config_file} " )
    @logger.debug("log_file: #{ @log_file} " )
    @logger.debug("command_line_options: #{ @command_line_options} " )
    @logger.debug("record_dirs_to_load: #{ @record_dirs_to_load} " )
    
    @mandatory_config.each do |m| 
      if @config[m].nil?
        @logger.error("Mandatory config #{m} is missing")
        raise "Mandatory config #{m} is missing"
      end
    end

    @conf = {}
    instance_variables.each do |a|
      attr_sym = a.to_s.gsub("@","").to_sym
      @conf[attr_sym] = instance_variable_get(a)
    end

    @record_dirs_to_load.map! do |d|
      if Dir.glob(d).empty?
        d = (d.start_with?("/records/") || d.start_with?("/source_records")) ? d : "/records/#{d}"
      end
      "#{d}/**/" 
    end

    @conf

  end

  def updateconfig(field, value)
    unless @command_line_options[field]
      @config[field] = value
    else
      @logger.debug("no config update for #{field}. Was part of command line option " )
    end
  end

  # Load in the configuration file details, setting many object attributes.
  # def get_system_config(config_file = @config.config_file() ) 
  def get_system_config() 
    @config ||= Config

    @config.path = "#{File.dirname(__FILE__)}/../../config/"
    @config.config_file = @config_file

    instance_variables.each do |a|
      attr_sym = a.to_s.gsub("@","").to_sym
      unless @config[attr_sym].nil?
        instance_variable_set(a, @config[attr_sym].clone )
      end
    end
  end

  def get_command_line_options
    # @logger.debug("get_command_line_options")
    #Defines the UI for the user. Albeit a simple command-line interface.
    OptionParser.new do |o|
      o.banner = "Usage: #{$0} [options]"

      o.on("-l LOGFILE", "--log", "write log to file") { |log_file| @log_file = log_file; @command_line_options[:log_file] = log_file }

      #Passing in a config file.... Or you can set a bunch of parameters.
      o.on("-c CONFIG", "--config", "Configuration file.") { |config_file| @config_file = config_file; @command_line_options[:config_file] = config_file  }

      o.on("-e ESLASTIC_LOGFILE", "--log", "write elastic client log to file") { |client_logger| @client_logger = client_logger; @command_line_options[:client_logger] = client_logger  }

      o.on("-d DIRECTORY", "--dir", "Sub directory relative to ./records/ \"dir1,dir2,dir3/dir\"") { |record_dirs_to_load| @record_dirs_to_load = record_dirs_to_load.split(","); @command_line_options[:record_dirs_to_load] = record_dirs_to_load.split(",") }

      o.on("-r RECORD_ID_FILE_PATTERN", "--record_id_file_pattern", "filepattern to extract record id. Same id must be preprocessed. If missing no preprocessing will be done") { |record_id_file_pattern| @record_id_file_pattern = record_id_file_pattern; @record_id_file_pattern[:record_id_file_pattern] = record_id_file_pattern  }

      o.on("-p PATTERN", "--pattern", "file pattern of record-filename /.*\\\.json/") { |record_pattern| @record_pattern = record_pattern; @command_line_options[:record_pattern] = Regexp.new(record_pattern) }

      o.on("-t LOAD_TYPE", "--load_type", "action: update, reload or reindex") { |load_type| @load_type = load_type; @command_line_options[:load_type] = load_type }

      o.on("-u LAST_RUN", "--last_run_updates", "Time the command was last run") { |last_run_updates| @last_run_updates = last_run_updates; @command_line_options[:last_run_updates] = last_run_updates }


      #Help screen.
      o.on( '-h', '--help', 'Display this screen.' ) do
        puts o
        exit
      end
      o.parse!   
    end
  end

  def load_type
    @load_type  
  end

  def load_type=(value)
    if ["update","reload","reindex"].include? (value)
      @load_type = value
    else
      message = "Wrong option for load_type it must be update, reload or reindex" 
      @logger.warn message
      raise message
    end
  end

  def mailAuditReport (subject,  report , importance, config)
    from_name = "#{config[:es_cluster]} Debugging"
    from_address = config[:audit][:mailfrom]
    to_name = "#{config[:es_cluster]}  Auditor"
    to_address = config[:audit][:mailto]
    now = DateTime.now

    message = <<END_OF_MESSAGE
From: #{from_name} #{from_address}
To: #{to_name} #{to_address}
MIME-Version: 1.0
Content-type: text/html
Subject: #{subject}
importance: #{importance}
Date: #{ now }

<H1>#{config[:es_cluster]} Audit Report</H1>

#{report}

END_OF_MESSAGE
    Net::SMTP.start('smtp.kuleuven.be', 25, tls_verify: false) do |smtp|
        smtp.send_message message,
        from_address, to_address
    end
  end

  
  def update_bulk
    begin
      unless @direct_load 
        create_import_directory()
      end

      check_elastic()
      @current_alias = get_current_alias()
      @logger.info "current alias for #{ @es_index } is #{@current_alias}"

      @logger.info "record_id_file_pattern #{ @record_id_file_pattern }"

      if @record_id_file_pattern.nil?
        load_bulk()
      else
        prepare_and_bulk()
      end
            
      unless @direct_load 
        remove_import_directory()
      end

    rescue StandardError => e
      @logger.error e
      raise e
    end
  end

  def reload_bulk
    begin

      unless @direct_load 
        create_import_directory()
      end

      check_elastic()
      new_index = create_index()
      @current_alias = new_index

      if @record_id_file_pattern.nil?
        load_bulk()
      else
        "Merge is necessary for adding datasets: example twitter"
        prepare_and_bulk()
      end

      @current_alias = get_current_alias()
      @logger.info "Replace alias for #{ @es_index }  from #{@current_alias} to #{new_index}"
            
      retval = @es_client.indices.put_alias index: new_index, name: @es_index
      if retval['errors']
        @logger.error retval
        raise "Errors in creating alias: #{retval['errors']}"
      end

      unless new_index == @current_alias
        delete_index
      end

      unless @direct_load 
        remove_import_directory()
      end

    rescue StandardError => e
      @logger.error e
      raise e
    end
  end

  def reindex
    begin
      check_elastic()
      @current_alias = get_current_alias() 
      new_index = create_index()
      
      @logger.info "change settings before reindexing (0 replicas, refresh_interval = -1"

      @current_es_settings = get_es_settings( new_index )
      pp get_es_settings( new_index )

      new_settings = {
        "index": {
         "number_of_replicas": 0,
          "refresh_interval": "-1"
        }
      }

      set_es_settings( new_index, new_settings )

      @logger.info "Reindex to index #{new_index}"

    # task_id =  @es_client.reindex(body: { source: { index: @current_alias }, dest: { index: new_index } }, wait_for_completion: false, refresh: true)['task']
      task_id =  @es_client.reindex(body: { source: { index: @current_alias, size: 300  }, dest: { index: new_index, pipeline: @es_pipeline_id } }, wait_for_completion: false, requests_per_second: 300, refresh: true)['task']

      @logger.info "Reindex => task_id: #{task_id}"

      #puts @es_client.tasks.list["nodes"]
      #puts @es_client.tasks.list(wait_for_completion: false)["nodes"]

      finished = false

      until finished do
        sleep  20
        @logger.debug "Reindex => task_id: #{task_id}"
      # ft =  @es_client.tasks.list task_id: task_id
        ft =  @es_client.tasks.get task_id: task_id
        @logger.debug "Reindex =>  ft: #{ft}"
        finished = ft["completed"]
      end

      reset_setting = {
        "index": {
         "number_of_replicas": @current_es_settings["index"]["number_of_replicas"],
          "refresh_interval": @current_es_settings["index"]["refresh_interval"]
        }
      }
      
      @logger.info "reset settings after reindexing (#{@current_es_settings["index"]["number_of_replicas"]} replicas, refresh_interval = #{@current_es_settings["index"]["refresh_interval"]}"
      set_es_settings( new_index, reset_setting )

      @logger.info "Replace alias for #{ @es_index }  from #{@current_alias} to #{new_index}"
      retval = @es_client.indices.put_alias index: new_index, name: @es_index
      if retval['errors']
        @logger.error retval
        raise "Errors in creating alias: #{retval['errors']}"
      end

      unless new_index == @current_alias
        delete_index
      end

    rescue StandardError => e
      @logger.error e
      raise e
    end
  end

  
  def load_enrichtment
    # Reads enrichment JSON files from @record_dirs_to_load.
    # Each enrichment record must contain a _source.@id field, identifying the target record to be enriched. 
    # Transformation of _source.@id to exactly match the iCANDID id are set in enrichment.record_id_transformation
    # Retrieves the corresponding records from the Elasticsearch index using these IDs and stores them in es_doc.
    # Passes the enrichment data to the rule engine via options[:enrichment].
    # Executes the rule set defined in @conf[:rule_set] using rules_ng.run(...), applying enrichment logic to the retrieved records.
    # The @conf[:enrichment][:rule_set] is used while parsing the @conf[:rule_set] it is pased to in as an option
    # Appends the enriched output to @jsonoutput.
    # Finally, updates the Elasticsearch index with the enriched records using process_bulk().
    #
    # Example config.yml 
    #   :rule_set: GOOGLE_AI_RULE_SET_v1_0
    #   :enrichment:
    #     :rule_set: GOOGLE_AI_TRANLATION_v1_0
    #     :options:
    #

    begin
      @total_nr_of_bulk_files = 0
      @total_nr_of_processed_files = 0


      check_elastic()
      @current_alias = get_current_alias()
      @logger.info "current alias for #{ @es_index } is #{@current_alias}"

      lastrun = Time.parse(@last_run_updates)
      @logger.debug "lastrun        : #{ lastrun}"
      @logger.debug "@direct_load   : #{ @direct_load}"
      @logger.debug "@last_run_updates : #{@last_run_updates}"
      @logger.debug "@es_index         : #{@es_index}"
      @logger.debug "@es_pipeline_id   : #{@es_pipeline_id}"
      @logger.debug "@record_dirs_to_load  : #{ @record_dirs_to_load}"
      @logger.debug "@record_pattern   : #{ @record_pattern}"
      @logger.debug "@current_alias    : #{ @current_alias}"

      @record_dirs_to_load.each do |records_dir|
        @logger.info "Start processing files in #{records_dir} [#{lastrun} < File.mtime]"

        files = Dir["#{records_dir}/*", "#{records_dir}/**/*"].select {|x| x =~ @record_pattern } 
        files.uniq!
        @logger.info " number of files to process  : #{files.size} "

        @nr_of_processed_files = 0
        @nr_of_old_files = 0
        @nr_of_bulk_files = 0

        unless files.size == 0
          @jsonoutput = []
          new_files_in_this_bulk = []
          files.each do |jsonfile|
            if lastrun < File.mtime(jsonfile) 
              # @logger.debug jsonfile

              data = JSON.parse( File.read("#{jsonfile}") )

              options = { :enrichment => data }
    
              new_files_in_this_bulk << jsonfile if jsonfile =~ /\/new\//
            
              unless @config[:enrichment][:record_id_transformation].nil?
                recordid = data['_source']['@id'].gsub( Regexp.new( @config[:enrichment][:record_id_transformation][:search]),  @config[:enrichment][:record_id_transformation][:replace])
              else
                recordid = data['_source']['@id']
              end

              es_doc = get_document_by_id( index: @current_alias , id: "#{ recordid }" )
             
              if es_doc.nil?
                pp "#{ recordid } [#{ data['_source']['@id'] }] RECORD NOT FOUND in #{@current_alias}"
                exit
              end
              @logger.info "doc_id : #{es_doc['@id']} "

             
              options[:rule_set] = @config[:enrichment][:rule_set]

              @config[:enrichment][:options].each { |k,v|
                options[k] = v  
              }
              
              #pp "ddddddddddddddddddddddddddddddddddddddddddd"
              #pp es_doc["prov:wasAttributedTo"][0]["prov:wasAssociatedFor"][0]["@id"]
              #pp "ddddddddddddddddddddddddddddddddddddddddd"

              rules_ng.run(  @conf[:rule_set].constantize[:rs_records], es_doc, output, options )

              jsondata =  output[:records]

              #pp "ddddddddddddddddddddddddddddddddddddddddd"
              #pp jsondata['@id']
              #pp jsondata["prov:wasAttributedTo"][0]["prov:wasAssociatedFor"][0]["@id"]
              #pp "ddddddddddddddddddddddddddddddddddddddddd"              

              # pp jsondata['@id']
              @jsonoutput << {"index":{
                "_index": @current_alias,
                "pipeline": @es_pipeline_id,
                "_id": jsondata['@id']
                }}

              @jsonoutput << jsondata

              # Enrichments can affect the same record. therefore, each record must be re-added to the index before the next enrichment can be added

              @nr_of_processed_files += 1

              process_bulk()
              move_records(new_files_in_this_bulk)
              new_files_in_this_bulk = []
              @jsonoutput = []
            end
          end

          @logger.debug "End processing files in #{records_dir}"
          @total_nr_of_processed_files += @nr_of_processed_files
        end
      end

      if @total_nr_of_processed_files > 0
        @logger.info "Total number of processed files : #{@total_nr_of_processed_files} chopped in #{@total_nr_of_bulk_files} pieces"
      else
        @logger.info "There was nothing to load or create !"
      end
      
    rescue StandardError => e
      @logger.error e
      raise e
    end
  end
  def move_records(new_files_in_this_bulk)
    begin
      unless new_files_in_this_bulk.nil?
        new_files_in_this_bulk.each do |file|
          @logger.info ("move #{file}")
          FileUtils.mkdir_p(File.dirname( file.gsub('/new/', '/processed/')  ))
          FileUtils.mv(file, file.gsub('/new/', '/processed/'))
        end
      end
    rescue StandardError => e
      @logger.error e
      raise e
    end
  end





  private

  def create_import_directory
    FileUtils.mkdir_p @temp_output_dir unless Dir.exist?(@temp_output_dir)
    clear_import_directory()
  end

  def clear_import_directory
    @logger.info "clear and remove temporary output directory! #{@temp_output_dir}"
    Dir.glob("#{@temp_output_dir}/*").each { |file| File.delete(file)}
  end

  def remove_import_directory
    FileUtils.rm_rf(@temp_output_dir)
  end

# ES will override records when using a bulk index load.
# A bulk create load can't handle updates
# A bulk update load doesn't support the pipeline functionality and override nested hashes or arrays
# Update by query does support pipiline but still the nested hashes or arrays could be a problem
# Therefor the records must be combined before loading.
# 
# Take the id's from the file names to remove duplicates
# Loop over the filtered list and parse all the file (duplicates) of that id to jsondata
# If the record already exists in ES combine this records with jsondata

  def prepare_and_bulk
    begin
      @total_nr_of_bulk_files = 0
      @total_nr_of_processed_files = 0

      lastrun = Time.parse(@last_run_updates)
      @logger.debug "Prepare_bulk in #{ @temp_output_dir }"
      @logger.debug "lastrun        : #{ lastrun }"
      @logger.debug "@last_run_updates : #{@last_run_updates}"
      @logger.debug "@record_dirs_to_load  : #{ @record_dirs_to_load}"
      @logger.debug "@record_pattern   : #{ @record_pattern}"
      @logger.debug "@record_id_file_pattern : #{ @record_id_file_pattern}"

      @record_dirs_to_load.each do |records_dir|
        @logger.info "Start prepare/processsing files in #{records_dir} [#{lastrun} < File.mtime]"

        files = Dir["#{records_dir}/*"].select { |jsonfile| jsonfile =~ @record_pattern && lastrun < File.mtime(jsonfile) }
      
        @logger.info " number of files to prepare/processsing : #{files.size}"

        @nr_of_processed_files = 0
        @nr_of_bulk_files = 0
        @jsonoutput = []
        
        unless files.size == 0

          #files_ids = files.map{ |f| File.basename( f, '.json' ).gsub(/iCANDID_twitter_([^\-\.]*).*/, '\1') }
          #files_ids.uniq!.sort!
          files_ids = files.inject({}) do |files_ids, f|
            if File.file?(f)
              #filename = File.basename( f, '.json' ).gsub(/iCANDID_twitter_([^_\-\.]*).*/, '\1')
              #@record_id_file_pattern = Regexp.new('iCANDID_twitter_([^_\-\.]*).*')
                           
              filename = File.basename( f, '.json' ).gsub( Regexp.new( @record_id_file_pattern ), '\1').gsub( '-include', '')

              if files_ids[filename].nil?
                files_ids[filename] = [f]
              else
                @logger.info ( "multiple files for: #{filename}" )
                files_ids[filename] << f
              end

            end
            files_ids
          end

          # files that are located in a directory structure that contains "new"
          # will be moved to the directory where "new" is replaced by "processed" 
          new_files_in_this_bulk = []

          files_ids.each_key do |files_id|
            # files_to_process = files.select { |f| f =~ /iCANDID_twitter_#{files_id}/ }
            files_to_process = files_ids[files_id]

            new_files_in_this_bulk.concat files_to_process.select { |f| f =~ /\/new\// } 

            jsondata = preprocess_records( files_to_process.sort)

            # Retrieve the records from ES and merge if it exists
            # only necessary if a record can be part of multipe datasets
            # or if the record is not completely loaded (example reftweets from twitter)
            
            doc_id = get_document_by_id( index: @current_alias , id: jsondata['@id'] )
            unless doc_id.nil?
              @logger.info ( "retrieved from elastic #{ jsondata['@id'] }" )
              unless doc_id["@uuid"].nil?
                jsondata['@uuid'] = doc_id["@uuid"]
              end
            end

            jsondata_from_file = jsondata.clone

            jsondata = create_record(jsondata)
            @logger.debug ( "record created for upload #{ jsondata['@id'] }" )     
            unless doc_id.nil?

              one_merged_record_providers = @config[:one_merged_record_providers].select { |p| jsondata['@id'].downcase =~ /^icandid_#{p.downcase}/  }
    
              if one_merged_record_providers.size > 0
                @logger.info ("Merge records for #{one_merged_record_providers}")
                jsondata = merge_json([jsondata,doc_id])
                jsondata = create_record(jsondata)
              end

              if jsondata['@id'].start_with?('iCANDID_twitter_')
                if jsondata["author"].is_a?(Array) && jsondata["author"].size > 1
                  jsondata["author"].select!{ |p|
                    if jsondata_from_file["author"].is_a?(Array)
                      jsondata_from_file["author"].map{|a| a["@id"]}.include?(p["@id"])
                    else
                      p["@id"] == jsondata_from_file["author"]["@id"]
                    end
                  }
                end
                if jsondata["creator"].is_a?(Array) && jsondata["creator"].size > 1
                  jsondata["creator"].select!{ |p|
                    if jsondata_from_file["creator"].is_a?(Array)
                      jsondata_from_file["creator"].map{|a| a["@id"]}.include?(p["@id"])
                    else
                      p["@id"] == jsondata_from_file["creator"]["@id"]
                    end
                  }
                end
                if jsondata["sender"].is_a?(Array) && jsondata["sender"].size > 1
                  jsondata["sender"].select!{ |p|
                    if jsondata_from_file["sender"].is_a?(Array)
                      jsondata_from_file["sender"].map{|a| a["@id"]}.include?(p["@id"])
                    else
                      p["@id"] == jsondata_from_file["sender"]["@id"]
                    end
                  }
                end

                jsondata = create_record(jsondata)
              end
            end

            # Create Bulk Record Load
            @jsonoutput << {"index":{
              "_index": @current_alias, 
            #  "_type": "_doc", 
              "pipeline": @es_pipeline_id,
              "_id": jsondata['@id']
              }}
            @jsonoutput << jsondata

            @nr_of_processed_files += 1
            @total_nr_of_processed_files += 1

            if @nr_of_processed_files > 0 && @nr_of_processed_files % @max_records_per_file == 0
              process_bulk()
              move_records(new_files_in_this_bulk)
              new_files_in_this_bulk = []
              @jsonoutput = []
            end
          end
        end

        process_bulk()
        move_records(new_files_in_this_bulk)
        @logger.debug "End processing files in #{records_dir}"
      end

      if @total_nr_of_processed_files > 0
        @logger.info "Total number of processed files : #{@total_nr_of_processed_files} chopped in #{@total_nr_of_bulk_files} pieces"
      else
        @logger.info "There was nothing to load or create !"
      end
      
    rescue StandardError => e
      @logger.error e
      raise e
    end
  end

  def load_bulk
    begin
      @total_nr_of_bulk_files = 0
      @total_nr_of_processed_files = 0

      lastrun = Time.parse(@last_run_updates)
      @logger.debug "lastrun        : #{ lastrun}"
      @logger.debug "@direct_load   : #{ @direct_load}"
      @logger.debug "@last_run_updates : #{@last_run_updates}"
      @logger.debug "@es_index         : #{@es_index}"
      @logger.debug "@es_pipeline_id   : #{@es_pipeline_id}"
      @logger.debug "@record_dirs_to_load  : #{ @record_dirs_to_load}"
      @logger.debug "@record_pattern   : #{ @record_pattern}"
      @logger.debug "@current_alias    : #{ @current_alias}"

      @record_dirs_to_load.each do |records_dir|
        @logger.info "Start processing files in #{records_dir} [#{lastrun} < File.mtime]"

        files = Dir["#{records_dir}/*", "#{records_dir}/**/*"].select {|x| x =~ @record_pattern } 

        @logger.info " number of files to process  : #{files.size} "

        @nr_of_processed_files = 0
        @nr_of_old_files = 0
        @nr_of_bulk_files = 0

        unless files.size == 0
          @jsonoutput = []
          new_files_in_this_bulk = []
          files.each do |jsonfile|
            if lastrun < File.mtime(jsonfile) 
              # @logger.debug jsonfile
             
              data = JSON.parse( File.read("#{jsonfile}") )
              jsondata = create_record(data)

              new_files_in_this_bulk << jsonfile if jsonfile =~ /\/new\//

              pp  jsondata['@id']
              # Create Bulk Record Load
              #@jsonoutput << {"index":{
              #  "_index": @current_alias,
              #  "_type": "_doc",
              #  "pipeline": @es_pipeline_id,
              #  "_id": jsondata['@id']
              #  }}
              @jsonoutput << {"index":{
                "_index": @current_alias,
                "pipeline": @es_pipeline_id,
                "_id": jsondata['@id']
                }}

              @jsonoutput << jsondata

              @nr_of_processed_files += 1

              if @nr_of_processed_files > 0 && @nr_of_processed_files % @max_records_per_file == 0
                process_bulk()
                move_records(new_files_in_this_bulk)
                new_files_in_this_bulk = []
                @jsonoutput = []
              end
            else
              @nr_of_old_files += 1
            end 
          end

          process_bulk()
          move_records(new_files_in_this_bulk)
          if files.size != (@nr_of_processed_files+ @nr_of_old_files)
              raise "While parsing #{records_dir}\n Number of files to process difference from the number of records that has been loaded to ElasticSearch !\n"
          end
          @logger.debug "End processing files in #{records_dir}"
          @total_nr_of_processed_files += @nr_of_processed_files
        end
      end

      if @total_nr_of_processed_files > 0
        @logger.info "Total number of processed files : #{@total_nr_of_processed_files} chopped in #{@total_nr_of_bulk_files} pieces"
      else
        @logger.info "There was nothing to load or create !"
      end
      
    rescue StandardError => e
      @logger.error e
      raise e
    end
  end




  def move_records(new_files_in_this_bulk)
    begin
      unless new_files_in_this_bulk.nil?
        new_files_in_this_bulk.each do |file|
          @logger.info ("move #{file}")
          FileUtils.mkdir_p(File.dirname( file.gsub('/new/', '/processed/')  ))
          FileUtils.mv(file, file.gsub('/new/', '/processed/'))
        end
      end
    rescue StandardError => e
      @logger.error e
      raise e
    end
  end


  def process_bulk
    unless @jsonoutput.empty?
      @total_nr_of_bulk_files += 1
      @nr_of_bulk_files += 1
      if @direct_load
        load_to_es(@jsonoutput, @es_client, @logger)
        @logger.debug "Loaded #{@nr_of_processed_files} records to #{@current_alias} in #{ @nr_of_bulk_files} load-actions"
      else
        to_jsonfile(@jsonoutput, @temp_output_file , @temp_output_dir)
        @logger.debug "Created #{@nr_of_processed_files} records in #{ @nr_of_bulk_files} files"
      end
    else
      @logger.debug "jsonoutput is empty no load or file creation needed "
    end
    @logger.debug " -> @nr_of_processed_files : #{@nr_of_processed_files}"
  end
end