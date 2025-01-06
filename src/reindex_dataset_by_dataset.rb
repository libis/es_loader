#encoding: UTF-8

#
# executes a Query (match all) with an aggregation on field_to_compare (isBasedOn.isPartOf.@id)
# in INDEX1 (source) and INDEX2 (destination)
# If a bucket key of the aggregation is missing in INDEX2 a reindexing with query ("isBasedOn.isPartOf.@id")
# is started. Also with an script and pipeline
#

$LOAD_PATH << '.' << './lib'
require 'logger'
require_relative './lib/loader'

##########

INDEX1 = "icandid_20240618"
INDEX2 = "icandid_20250101"

#field_to_compare = "isBasedOn.provider.@id"
field_to_compare = "isBasedOn.isPartOf.@id"
script_source = "if (ctx._source.datePublished != null) { try { def dateValue = ZonedDateTime.parse(ctx._source.datePublished); ctx._source._datePublished = dateValue.toString(); } catch (Exception e) {  } }"

begin

  #output_dir = "/elastic/import"
  # every docker-container has its own HOSTNAME
  output_dir = "/elastic/import/#{ENV["HOSTNAME"]}"
  output_file = "import.bulk"

  #Create a loader object.
  loader = Loader.new()
  config = loader.config()

  loader.logger.info "use utils for es_cluster : #{loader.es_cluster}"
  loader.check_elastic()

  @es_url = config[:es_url]
  
  @es_client = Elasticsearch::Client.new url: @es_url, transport_options: {  ssl:  { verify: false } }
  
 # client.search(index: 'my_index', body: { query: { match_all: {} } })
 result_1 = loader.search(index: INDEX1, body: { size: 0, track_total_hits: true, aggs: { field_to_compare: { terms: { field: field_to_compare, size: 1000, order: { _key: "asc" } } } } })
 result_2 = loader.search(index: INDEX2, body: { size: 0, track_total_hits: true, aggs: { field_to_compare: { terms: { field: field_to_compare, size: 1000, order: { _key: "asc" } } } } })

 loader.logger.info  "--------------------------------------------"
 loader.logger.info  ""

 result_1["aggregations"]["field_to_compare"]["buckets"].each { |agg_1| 
   agg_2 = result_2["aggregations"]["field_to_compare"]["buckets"].select { |s| s["key"] ==  agg_1["key"]}.first
   if agg_2.nil?
    dataset = agg_1["key"]
    loader.logger.info  " reindex #{dataset} from #{INDEX1} to #{INDEX2}"

      task_id =  @es_client.reindex(
        body: { 
          source: { index: INDEX1, query: {bool: {must: [ {term: {  "#{field_to_compare}": { value: dataset } }} ]}} }, 
          dest: { index: INDEX2  }, 
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
          sleep 20
        end


    else
      if agg_1[ "doc_count"] == agg_2[ "doc_count"]
        loader.logger.info  " #{agg_1["key"]} : #{agg_1[ "doc_count"] } OK"
      else
        loader.logger.info  "==============>  Not the same amount of records for #{agg_1["key"]}"
        loader.logger.info  "   #{agg_1["key"]} : #{agg_1[ "doc_count"] } records (#{INDEX1})"
        loader.logger.info  "   #{agg_2["key"]} : #{agg_2[ "doc_count"] } records (#{INDEX2})"
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
























