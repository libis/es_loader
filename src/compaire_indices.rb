#encoding: UTF-8
$LOAD_PATH << '.' << './lib'
require 'logger'
require_relative './lib/loader'

##########

INDEX1 = "icandid_v2_2_20240208_with_prov"
INDEX2 = "icandid_20240320"
 
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
  
  # client.search(index: 'my_index', body: { query: { match_all: {} } })
  result_1 = loader.search(index: INDEX1, body: { size: 0, track_total_hits: true, aggs: { provider_id: { terms: { field: "isBasedOn.provider.@id", size: 1000, order: { _key: "asc" } } } } })
  result_2 = loader.search(index: INDEX2, body: { size: 0, track_total_hits: true, aggs: { provider_id: { terms: { field: "isBasedOn.provider.@id", size: 1000, order: { _key: "asc" } } } } })

  pp "--------------------------------------------"
  pp ""

  result_1["aggregations"]["provider_id"]["buckets"].each { |agg_1| 
    agg_2 = result_2["aggregations"]["provider_id"]["buckets"].select { |s| s["key"] ==  agg_1["key"]}.first
    if agg_1[ "doc_count"] == agg_2[ "doc_count"]
      pp " #{agg_1["key"]} : #{agg_1[ "doc_count"] } OK"
    else
      pp "==============>  Not the same amount of records for #{agg_1["key"]}"
      pp "   #{agg_1["key"]} : #{agg_1[ "doc_count"] } records (#{INDEX1})"
      pp "   #{agg_2["key"]} : #{agg_2[ "doc_count"] } records (#{INDEX2})"
    end
  
  }

  pp ""
  pp "--------------------------------------------"

rescue StandardError => e
  
  pp "--> RESCUE StandardError  "

  pp e
  exit

ensure


  pp "--> ENSURE "
  exit


end
























