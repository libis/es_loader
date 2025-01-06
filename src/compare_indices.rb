#encoding: UTF-8
$LOAD_PATH << '.' << './lib'
require 'logger'
require_relative './lib/loader'

##########

INDEX1 = "icandid_20240618"
INDEX2 = "icandid_20250101"

#field_to_compare = "isBasedOn.provider.@id"
field_to_compare = "isBasedOn.isPartOf.@id"
 
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
  
prefixes = [  
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_h",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_ha",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_hb",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_hc",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_h6",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_h8",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_h9",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_HA1",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_HA",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_HB",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_H6",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_H7",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_H8",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_H9",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_H9A",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_H9B",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_H9D",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_H90",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_H91",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_s",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_z",
"iCANDID_zweedsparlement_data_riksdagen_query_0000001_Z"
]

=begin
prefixes.each  { |prefix| 

  result_1 = loader.search(index: INDEX1, body: { size: 0, track_total_hits: true, query: { prefix: { "@id": { value: prefix } } } })
  result_2 = loader.search(index: INDEX2, body: { size: 0, track_total_hits: true, query: { prefix: { "@id": { value: prefix } } }})

  #GET INDEX1/_search?filter_path=hits.total
  if ( result_1["hits"]["total"]["value"] != result_2["hits"]["total"]["value"] )
    pp prefix
    pp "#{result_1["hits"]["total"]["value"]} #{result_2["hits"]["total"]["value"]}"
  end

}
=end

  # client.search(index: 'my_index', body: { query: { match_all: {} } })
  result_1 = loader.search(index: INDEX1, body: { size: 0, track_total_hits: true, aggs: { field_to_compare: { terms: { field: field_to_compare, size: 1000, order: { _key: "asc" } } } } })
  result_2 = loader.search(index: INDEX2, body: { size: 0, track_total_hits: true, aggs: { field_to_compare: { terms: { field: field_to_compare, size: 1000, order: { _key: "asc" } } } } })

  pp "--------------------------------------------"
  pp ""

  result_1["aggregations"]["field_to_compare"]["buckets"].each { |agg_1| 
    agg_2 = result_2["aggregations"]["field_to_compare"]["buckets"].select { |s| s["key"] ==  agg_1["key"]}.first
    if agg_2.nil?
      #pp "#{agg_1} is missing in #{INDEX2}"
    else
      if agg_1[ "doc_count"] == agg_2[ "doc_count"]
        pp " #{agg_1["key"]} : #{agg_1[ "doc_count"] } OK"
      else
        pp "==============>  Not the same amount of records for #{agg_1["key"]}"
        pp "   #{agg_1["key"]} : #{agg_1[ "doc_count"] } records (#{INDEX1})"
        pp "   #{agg_2["key"]} : #{agg_2[ "doc_count"] } records (#{INDEX2})"
      end
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
























