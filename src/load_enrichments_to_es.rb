#encoding: UTF-8
$LOAD_PATH << '.' << './lib'
require 'logger'
require_relative './lib/loader'


ROOT_PATH = File.join( File.dirname(__FILE__), '../')

##########

begin
  #output_dir = "/elastic/import"
  # every docker-container has its own HOSTNAME
  output_dir = "/elastic/import/#{ENV["HOSTNAME"]}"
  output_file = "import.bulk"

  #Create a loader object.
  loader = Loader.new()
  config = loader.config()

  loader.logger.info "use utils for es_cluster : #{loader.es_cluster}"

  loader.logger.info "load_type : #{loader.load_type}"

  loader.check_elastic()

  loader.direct_load = true

  start_parsing = DateTime.now.to_time
  
  case loader.load_type
  when "enrichtment"

    Dir[  File.join( ROOT_PATH,"src/rules/*.rb") ].each {|file| require file; }
 
    last_run = loader.last_run_updates

    #total_nr_of_bulk_files = 0
    loader.logger.info "update ES config last run : #{last_run}"

    if config[:rule_set].nil?
      message = "loading enrichments requires a rule_set to parse the enrichment and combine it with the records from Elastic"
      loader.logger.warn message
      loader.logger.warn "add :rule_set: to config.yml"
      message = message + "\nadd :rule_set: to config.yml"
      raise message
    end

    loader.load_enrichtment()

  else
    message = "Wrong option for load_type it must be update, reload or reindex" 
    loader.logger.warn message
    raise message
  end

  loader.updateconfig(:last_run_updates, start_parsing.to_s)

rescue StandardError => e
  puts e.message
  puts e.backtrace.inspect

  loader.logger.error e

  importance = "High"
  subject = "[ERROR] #{loader.es_cluster} ES Loader report"
  message = <<END_OF_MESSAGE

  <h2>Error in #{loader.load_type} load_to_es for #{loader.es_index} in cluster [ #{loader.es_cluster}]</h2>
  <p>#{e.message}</p>
  <p>#{e.backtrace.inspect}</p>

  <hr>

  load_type:           #{loader.load_type}</br>
  es_cluster:          #{loader.es_cluster}</br>
  es_index:            #{loader.es_index}</br>
  record_dirs_to_load: #{loader.record_dirs_to_load}</br>
  record_pattern:      #{loader.record_pattern}</br>
  import_mappings:     #{loader.import_mappings}</br>
  import_settings:     #{loader.import_settings}</br>
  import_pipeline:     #{loader.import_pipeline}</br>

END_OF_MESSAGE

loader.mailAuditReport(subject, message, importance, config)

ensure

case loader.load_type
when "update"
  
  importance = "Normal"
  subject = "#{loader.es_cluster} ES Loader [#{loader.load_type}] report [#{ loader.total_nr_of_processed_files}]"

  header = "load_to_es [#{loader.load_type}] in cluster #{loader.es_cluster}, index [#{loader.es_index}]"
  params = <<END_OF_PARAMS
  load_type:           #{loader.load_type}</br>
  es_index:            #{loader.es_index}</br>
  record_dirs_to_load: #{loader.record_dirs_to_load}</br>
  record_pattern:      #{loader.record_pattern}</br>

END_OF_PARAMS

  message = "" 
  if loader.direct_load
    message += " Loaded #{ loader.total_nr_of_processed_files} records to #{ loader.es_index} in #{  loader.total_nr_of_bulk_files} load-actions"
  else
    message += " Created #{ loader.total_nr_of_processed_files} records in #{  loader.total_nr_of_bulk_files} files"
  end

when "reload"
  
  importance = "Normal"
  subject = "#{ loader.es_cluster } ES Loader [#{loader.load_type}] report [#{ loader.total_nr_of_processed_files}]"

  header = "load_to_es [#{loader.load_type}] in cluster #{loader.es_cluster}, index [#{loader.es_index}]"
  params = <<END_OF_PARAMS
  load_type:           #{loader.load_type}</br>
  es_index:            #{loader.es_index}</br>
  record_dirs_to_load: #{loader.record_dirs_to_load}</br>
  record_pattern:      #{loader.record_pattern}</br>
  import_mappings:     #{loader.import_mappings}</br>
  import_settings:     #{loader.import_settings}</br>
  import_pipeline:     #{loader.import_pipeline}</br>

END_OF_PARAMS

  message = " Reloaded records, previous records were removed\n\n New mapping, setting and pipeline !!!" 
  if loader.direct_load
    message = " Loaded #{ loader.total_nr_of_processed_files} records to #{ loader.es_index} in #{  loader.total_nr_of_bulk_files} load-actions"
  else
    message = " Created #{ loader.total_nr_of_processed_files} records in #{  loader.total_nr_of_bulk_files} files"
  end

when "reindex"
  importance = "Normal"
  subject = "#{ loader.es_cluster } ES Loader [#{loader.load_type}] report"
 
  header = "load_to_es [#{loader.load_type}] in cluster #{loader.es_cluster}, index [#{loader.es_index}]"
  params = <<END_OF_PARAMS
  
  load_type:           #{loader.load_type}</br>
  es_cluster:          #{loader.es_cluster}</br>
  es_index:            #{loader.es_index}</br>
  import_mappings:     #{loader.import_mappings}</br>
  import_settings:     #{loader.import_settings}</br>
  import_pipeline:     #{loader.import_pipeline}</br>

END_OF_PARAMS

  message = " Reindexed the full index #{loader.es_index} of cluster #{loader.es_cluster}"
end

message = <<END_OF_MESSAGE

<h2>#{header}</h2>

#{params}
</br>
</br>
#{message}
</br>

END_OF_MESSAGE


loader.mailAuditReport(subject, message, importance, config)

end

