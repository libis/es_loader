#encoding: UTF-8
require 'yaml'
#
# executes a Query (match all) with an aggregation on field_to_compare (isBasedOn.isPartOf.@id)
# in source_index (source) and dest_index (destination)
# If a bucket key of the aggregation is missing in dest_index a reindexing with query ("isBasedOn.isPartOf.@id")
# is started. Also with an script and pipeline
#

$LOAD_PATH << '.' << './lib'
require 'logger'
require_relative './lib/loader'

##########

#field_to_compare = "isBasedOn.provider.@id"
field_to_compare = "isBasedOn.isPartOf.@id"
#script_source = "if (ctx._source.datePublished != null) { try { def dateValue = ZonedDateTime.parse(ctx._source.datePublished); ctx._source._datePublished = dateValue.toString(); } catch (Exception e) {  } }"
#script_source = "if (ctx._source.datePublished != null) { try { def dateValue = LocalDate.parse(ctx._source.datePublished); ctx._source._datePublished = dateValue.toString(); } catch (Exception e) {  } }"

begin

  #output_dir = "/elastic/import"
  # every docker-container has its own HOSTNAME
  output_dir = "/elastic/import/#{ENV["HOSTNAME"]}"
  output_file = "import.bulk"

  #Create a loader object.
  loader = Loader.new()
  loader_config = loader.config()

  pp loader.config_file
  config = YAML::load_file("#{File.dirname(__FILE__)}/../config/#{loader.config_file}", permitted_classes: [Symbol, Regexp])
  
  if config[:source_index].nil? || config[:dest_index].nil?
    raise "config[:source_index] or config[:dest_index] are not defined in #{loader.config_file}"
    exit(0)
  end
  source_index = config[:source_index]
  dest_index = config[:dest_index]
  script_source = config[:script_source]


  pp script_source
  exit
  loader.logger.info "use utils for es_cluster : #{loader.es_cluster}"
  loader.check_elastic()

  @es_url = config[:es_url]
  
  @es_client = Elasticsearch::Client.new url: @es_url, transport_options: {  ssl:  { verify: false } }
  
 # client.search(index: 'my_index', body: { query: { match_all: {} } })
 source_result = loader.search(index: source_index, body: { size: 0, track_total_hits: true, aggs: { field_to_compare: { terms: { field: field_to_compare, size: 1000, order: { _key: "asc" } } } } })
 dest_result = loader.search(index: dest_index, body: { size: 0, track_total_hits: true, aggs: { field_to_compare: { terms: { field: field_to_compare, size: 1000, order: { _key: "asc" } } } } })

 loader.logger.info  "--------------------------------------------"
 loader.logger.info  ""

 source_result["aggregations"]["field_to_compare"]["buckets"].each { |source_agg| 
   dest_agg = dest_result["aggregations"]["field_to_compare"]["buckets"].select { |s| s["key"] ==  source_agg["key"]}.first
   
    dataset = source_agg["key"]
    loader.logger.info  " reindex #{dataset} from #{source_index} to #{dest_index}"
    if dest_agg.nil?
      task_id =  @es_client.reindex(
        body: { 
          source: { index: source_index, query: {bool: {must: [ {term: {  "#{field_to_compare}": { value: dataset } }} ]}} }, 
          dest: { index: dest_index  }, 
          conflicts: "proceed",
          script: { source: script_source }
        },
        wait_for_completion: false, 
        requests_per_second: 300, 
        refresh: true, )['task']

        loader.logger.info  "Reindex => task_id: #{task_id}"
        finished = false

      
        until finished do
          tasks = @es_client.tasks.list(actions: '*reindex' )
          filtered_task = nil
          tasks['nodes'].each do |node_id, node_info|
            node_info['tasks'].each do |tid, task_info|
              if tid == task_id # Replace with the specific task ID
                pp tid
                pp task_info
                filtered_task = task_info
                break
              end
            end
          end
          if filtered_task
            finished = false
          else
            finished = true
          end
          sleep 60
        end

    else
      if source_agg[ "doc_count"] == dest_agg[ "doc_count"]
        loader.logger.info  " #{source_agg["key"]} : #{source_agg[ "doc_count"] } OK"
      else
        loader.logger.info  "==============>  Not the same amount of records for #{source_agg["key"]}"
        loader.logger.info  "   #{source_agg["key"]} : #{source_agg[ "doc_count"] } records (#{source_index})"
        loader.logger.info  "   #{dest_agg["key"]} : #{dest_agg[ "doc_count"] } records (#{dest_index})"
      end
    end

  }

  loader.logger.info  ""
  loader.logger.info  "--------------------------------------------"

rescue StandardError => e
  
  pp "--> RESCUE StandardError  "

  pp e
  exit

ensure


  pp "--> ENSURE "
  exit


end
























