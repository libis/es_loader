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


  def check_elastic()
    if @es_url.to_s.empty?
      raise 'es_url not defined'
    end
    @logger.debug "es_url: #{ es_url }"

    if @log_es_client
      @es_client = Elasticsearch::Client.new url: @es_url, logger: @client_logger, log: true, transport_options: { ssl: { verify: false } }
    else
      @es_client = Elasticsearch::Client.new url: @es_url, transport_options: {  ssl:  { verify: false } }
    end

    health = @es_client.cluster.health
    @logger.debug "cluster.health.status: #{health['status']}"
    
    if @es_client.info['version']['number'] != @es_version
        message = "Wrong Elasticsearch version on server: #{ @es_client.info['version']['number'] } on server but expected #{ @es_version }"
        @logger.warn message
        raise message
    end

    unless health['status'] === 'green' || health['status'] === 'yellow'
      message = "ElasticSearch Health status not OK [ #{health['status']} ]"
      @logger.error message
      raise message
    end

  end

  def create_index

    new_index = "#{@es_index}_#{Time.now.to_i}"
    @logger.info "Create index: #{ new_index }"
    jsonsettings = JSON.parse( File.read("#{@import_settings}") )

    retval =  @es_client.indices.create(index: new_index, body:  { settings: { index: jsonsettings } } )
    errors = retval['errors']
    if retval['errors']
      @logger.error retval
      raise retval['errors']
    end

    unless @es_client.indices.exists? index: @es_index
      @logger.info "Create alias #{@es_index} for #{ new_index }"

      retval = @es_client.indices.put_alias index: new_index, name: @es_index
      if retval['errors']
        @logger.error retval
        raise "Errors in creating alias: #{retval['errors']}"
      end
    end

    set_es_mapping (new_index)
    set_es_pipeline (es_pipeline_id)

    return new_index

  end


  def get_es_settings (index)
    @logger.info "Get settings of index: #{index}"

    retval = @es_client.indices.get_settings( index: index)
    if retval['errors']
        log.error retval
        raise "Errors in mapping: #{retval['errors']}"
    else
      retval[index]["settings"]
    end
  end
  
  def set_es_settings (index, settings )
    @logger.info "Set settings of index: #{index}"

    retval = @es_client.indices.put_settings( index: index, body: settings)
    if retval['errors']
        log.error retval
        raise "Errors in set settings: #{retval['errors']}"
    end
  end




  def set_es_mapping (index)
    @logger.info "Import mapping #{@import_mappings} to index: #{index}"
    jsonmappings = JSON.parse( File.read("#{@import_mappings}") )

    retval = @es_client.indices.put_mapping index: index, body: jsonmappings
    if retval['errors']
        log.error retval
        raise "Errors in mapping: #{retval['errors']}"
    end
  end

  def set_es_pipeline (es_pipeline_id)
    @logger.info "Import pipeline #{es_pipeline_id}"
    jsonpipeline = JSON.parse( File.read("#{@import_pipeline}") )

    retval = @es_client.ingest.put_pipeline id: es_pipeline_id, body:  jsonpipeline
    if retval['errors']
        log.error retval
        raise "Errors in pipeline: #{retval['errors']}"
    end
  end

  def get_current_alias
    unless @es_client.indices.exists? index: @es_index  
      new_index = create_index
    end
    aliases = @es_client.indices.get_alias

    if aliases['errors']
        raise "Errors in get_alias: #{retval['errors']}"
    end

    current_aliases = aliases.select{|index_id, index_hash| index_hash["aliases"].keys.include? @es_index }
    current_index = current_aliases.keys.first 
    current_index
  end

  def check_index(index)
    @es_client.indices.exists? index: index
  end

  def get_document_by_id( index: @current_alias  , id: )
    retval = @es_client.get ({index: index, id: id, ignore: 404 } )
    if retval["found"]
      retval["_source"]
    else
      nil
    end
  end

  def load_to_es(jsondata, client, logger)
    unless jsondata.empty? 


#      puts JSON.pretty_generate(jsondata)

      logger.debug "load records to Elastic #{jsondata.size / 2}"
      retval = client.bulk body: jsondata
      
      if retval['errors']
        error_message = ["!!!! while inserting records !!!!!!!!!!!!!!!!!\nErrors in bulk: #{retval['errors']}"]
        
        retval['items'].each do |i|
          unless i['index']['error'].nil?
            error_message << "Error in bulk items #{i['index']['_id']} : i['update']['error']"
            raise "Error in bulk items : #{i}"
          end
        end
        raise error_message.join("\n")
      end
    end
  end


  def delete_index
    @logger.info "Delete alias #{@es_index} from #{@current_alias} "
        retval = @es_client.indices.delete_alias index: @current_alias, name: @es_index

        if retval['errors']
          @logger.error retval
          raise "Errors in deleting : #{retval['errors']}"
        end

        #@logger.info "Delete index #{@current_alias}"
        #retval =  @es_client.indices.delete index: @current_alias
        #if retval['errors']
        #  @logger.error retval
        #  raise "Errors in deleting : #{retval['errors']}"
        #end
  end

end