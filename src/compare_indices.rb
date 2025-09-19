#encoding: UTF-8
$LOAD_PATH << '.' << './lib'
require 'logger'
require_relative './lib/loader'

##########

SOURCE_URL = "https://icandid_admin:ibandid_admin@host.docker.internal:9292"
SOURCE_INDEX = "icandid_20250606"
TARGET_URL = "https://icandid_admin:ibandid_admin@host.docker.internal:9222"
TARGET_INDEX = "icandid"

#field_to_compare = "isBasedOn.provider.@id"
field_to_compare = "isBasedOn.isPartOf.@id"
 

# Helper to scroll and collect all IDs
def fetch_all_ids(client: nil, index: nil, body: { _source: false })
  ids = Set.new
  response = client.search(index: index, scroll: '2m', size: 1000, body: body)

  scroll_id = response['_scroll_id']
  hits = response['hits']['hits']
  ids.merge(hits.map { |doc| doc['_id'] })

  while hits.any?
    response = client.scroll(scroll_id: scroll_id, scroll: '2m')
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    ids.merge(hits.map { |doc| doc['_id'] })
  end

  ids
end




















begin
  

  #output_dir = "/elastic/import"
  # every docker-container has its own HOSTNAME
  output_dir = "/elastic/import/#{ENV["HOSTNAME"]}"
  output_file = "import.bulk"
  begin
    @es_source_client = Elasticsearch::Client.new url: SOURCE_URL, transport_options: {  ssl:  { verify: false }, request: { timeout: 120 } }
    @es_target_client = Elasticsearch::Client.new url: TARGET_URL, transport_options: {  ssl:  { verify: false }, request: { timeout: 120 } }
  rescue => e
    puts "Error creating Elasticsearch::Client: #{e.message}"
  end

  body = { size: 0, track_total_hits: true, aggs: { field_to_compare: { terms: { field: field_to_compare, size: 1000, order: { _key: "asc" } } } } }

  begin
    source_results = @es_source_client.search ({index: SOURCE_INDEX, body: body } )
  rescue => e
    puts "Error searching source : #{e.message}"
  end

  begin
    target_results = @es_target_client.search ({index: TARGET_INDEX, body: body } )
  rescue => e
    puts "Error searching target : #{e.message}"
  end

  source_results["aggregations"]["field_to_compare"]["buckets"].each { |source_result| 
    target_result = target_results["aggregations"]["field_to_compare"]["buckets"].select { |s| s["key"] ==  source_result["key"]}.first
    if target_result.nil?
      #pp "#{agg_1} is missing in #{INDEX2}"
    else
      if source_result[ "doc_count"] == target_result[ "doc_count"]
        pp " #{source_result["key"]} : #{source_result[ "doc_count"] } OK"
      else
        pp "==============>  Not the same amount of records for #{source_result["key"]}"
        pp "   #{source_result["key"]} : #{source_result[ "doc_count"] } records (#{SOURCE_URL}/#{SOURCE_INDEX})"
        pp "   #{target_result["key"]} : #{target_result[ "doc_count"] } records (#{TARGET_URL}/#{TARGET_INDEX})"

        body = { _source: false, query: { terms: { "#{field_to_compare}": [ "#{target_result["key"]}" ] } }}

        pp body
        if target_result["key"] == "belgapress_query_00026"
          # Fetch IDs
          puts "Fetching IDs from #{SOURCE_URL}/#{SOURCE_INDEX} ..."
          source_ids = fetch_all_ids(client: @es_source_client, index: SOURCE_INDEX, body: body)

          puts "Fetching IDs from #{TARGET_URL}/#{TARGET_INDEX} ..."
          target_ids = fetch_all_ids(client: @es_target_client, index: TARGET_INDEX, body: body)

          # Compare
          missing_ids = source_ids - target_ids

          # Output
          puts "\nMissing IDs in #{TARGET_URL}/#{TARGET_INDEX}:"
          missing_ids.each { |id| puts id }

          puts "\nTotal missing: #{missing_ids.size}"

       exit
        end

      end
    end
  
  }

  pp ""
  pp "--------------------------------------------"

rescue StandardError => e
  
  pp "--> RESCUE StandardError  "

  pp e
  exit

end
























