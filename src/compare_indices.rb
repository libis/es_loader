# frozen_string_literal: true
# encoding: UTF-8

require "logger"
require "set"
require "elasticsearch"
require_relative "./lib/loader"

SOURCE_URL  = "https://#{ENV['ES_USER']}:#{ENV['ES_PASSWORD']}@host.docker.internal:9202"
# TARGET_URL  = "https://#{ENV['ES_USER']}:#{ENV['ES_PASSWORD_iCANDID3']}@host.docker.internal:9203"
TARGET_URL  = "https://#{ENV['ES_USER']}:#{ENV['ES_PASSWORD']}@host.docker.internal:9202"
SOURCE_INDEX = "icandid_20251101"
TARGET_INDEX = "icandid_20260204"

FIELD_TO_COMPARE = "isBasedOn.isPartOf.@id"

OUTPUT_DIR = "/elastic/import/#{ENV['HOSTNAME']}"
OUTPUT_FILE = "import.bulk"


# -----------------------------------------
# Helper: Build Elasticsearch client
# -----------------------------------------
def build_es_client(url)
  Elasticsearch::Client.new(
    url: url,
    transport_options: {
      ssl: { verify: false },
      request: { timeout: 120 }
    }
  )
rescue => e
  abort "Error creating Elasticsearch client: #{e.message}"
end


# -----------------------------------------
# Helper: Scroll entire index for matching IDs
# -----------------------------------------
def fetch_all_ids(client:, index:, body:)
  ids = Set.new

  response = client.search(index: index, scroll: "2m", size: 1000, body: body)
  scroll_id = response["_scroll_id"]

  loop do
    hits = response.dig("hits", "hits") || []
    break if hits.empty?

    ids.merge(hits.map { |doc| doc["_id"] })

    response = client.scroll(scroll_id: scroll_id, scroll: "2m")
    scroll_id = response["_scroll_id"]
  end

  ids
end


# -----------------------------------------
# Helper: Run aggregation query
# -----------------------------------------
def aggregated_counts(client, index)
  body = {
    size: 0,
    track_total_hits: true,
    aggs: {
      field_to_compare: {
        terms: {
          field: FIELD_TO_COMPARE,
          size: 2000,
          order: { _key: "asc" }
        }
      }
    }
  }

  client.search(index: index, body: body)
rescue => e
  abort "Error querying index #{index}: #{e.message}"
end


# -----------------------------------------
# Compare two buckets
# -----------------------------------------
def compare_buckets(source_buckets, target_buckets)
  source_buckets.each do |src|
    key   = src["key"]
    count = src["doc_count"]

    tgt = target_buckets.find { |b| b["key"] == key }

    if tgt.nil?
      puts "!! Missing in target index: #{key} !!!!!!"
      next
    end

    target_count = tgt["doc_count"]

    if count == target_count
      puts "!! #{key} : #{count} OK"
    else
      puts "\n======> Mismatch for #{key}"
      puts "  Source: #{count} (#{SOURCE_INDEX})"
      puts "  Target: #{target_count} (#{TARGET_INDEX})"

      yield key if block_given?
    end
  end
end


# -----------------------------------------
# Missing ID Analysis
# -----------------------------------------
def analyze_missing_ids(key, source_client, target_client)
  puts "\n=> Fetching IDs for #{key}..."

  body = {
    _source: false,
    query: {
      terms: { FIELD_TO_COMPARE => [key] }
    }
  }

  source_ids = fetch_all_ids(client: source_client, index: SOURCE_INDEX, body: body)
  target_ids = fetch_all_ids(client: target_client, index: TARGET_INDEX, body: body)

  missing_ids = source_ids - target_ids

  puts "\nMissing IDs in #{TARGET_INDEX}:"
  missing_ids.each { |id| puts id }

  puts "\nTOTAL missing: #{missing_ids.size}\n"
end


# -----------------------------------------
# Main Process
# -----------------------------------------
begin
  pp " Main Process"
  pp SOURCE_URL
  source_client = build_es_client(SOURCE_URL)
  target_client = build_es_client(TARGET_URL)

  source_results = aggregated_counts(source_client, SOURCE_INDEX)
  target_results = aggregated_counts(target_client, TARGET_INDEX)

  source_buckets = source_results.dig("aggregations", "field_to_compare", "buckets")
  target_buckets = target_results.dig("aggregations", "field_to_compare", "buckets")

  compare_buckets(source_buckets, target_buckets) do |key|
    if key == "belgapress_query_00026"
      analyze_missing_ids(key, source_client, target_client)
      exit
    end
  end

  puts "\n--------------------------------------------"

rescue => e
  puts "\n--> ERROR"
  puts e.message
  puts e.backtrace
end