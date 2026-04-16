#!/usr/bin/env ruby


# ============================================================
# Elasticsearch Strict Index Clone & Validation Tool
# ============================================================
#
# This script clones an Elasticsearch index from a source cluster
# to a target cluster by copying index settings and mappings,
# enforcing a fully STRICT schema (dynamic: "strict" at all levels).
#
# Key features:
# - Fetches settings and mappings from a source index
# - Removes non-copyable/illegal index settings (UUIDs, versions, etc.)
# - Forces strict mappings on all object fields recursively
# - Optionally validates example documents using an existing ingest pipeline
# - Supports DRY-RUN mode for safe previews
# - Creates a date-suffixed physical target index
# - Assigns or updates an alias pointing to the new index
#
# Intended use:
# - Safe index recreation and promotion across clusters
# - Schema hardening before production ingestion
# - Pre-validation of documents against strict mappings
#
# Configuration is fully driven via environment variables.
# No changes are applied when DRY_RUN=true.
#
# ============================================================

require 'elasticsearch'
require 'securerandom'
require 'json'
require 'time'

# ============================================================
# CONFIGURATION (ENV VARS)
# ============================================================

CLUSTER_SOURCE_URL = ENV.fetch('CLUSTER_SOURCE_URL')
CLUSTER_TARGET_URL = ENV.fetch('CLUSTER_TARGET_URL')

SOURCE_ES_USER     = ENV.fetch('CLUSTER_SOURCE_ES_USER')
SOURCE_ES_PASSWORD = ENV.fetch('CLUSTER_SOURCE_ES_PASSWORD')

TARGET_ES_USER     = ENV.fetch('CLUSTER_TARGET_ES_USER')
TARGET_ES_PASSWORD = ENV.fetch('CLUSTER_TARGET_ES_PASSWORD')

SOURCE_INDEX = ENV.fetch('SOURCE_INDEX')
TARGET_INDEX = ENV.fetch('TARGET_INDEX')

TARGET_ALIAS = TARGET_INDEX
PHYSICAL_TARGET_INDEX = "#{TARGET_INDEX}_#{Time.now.strftime('%Y%m%d')}"

DRY_RUN = %w[true 1 yes].include?(ENV.fetch('DRY_RUN', 'false').downcase)

VALIDATE_DOCS = %w[true 1 yes].include?(ENV.fetch('VALIDATE_DOCS', 'false').downcase)
VALIDATION_DOCS_PATH = ENV.fetch('VALIDATION_DOCS_PATH', './test_docs')

TARGET_ES_PIPELINE_ID = ENV['TARGET_ES_PIPELINE_ID']

# ============================================================
# CLIENTS
# ============================================================

puts "🔎 DRY‑RUN MODE ENABLED" if DRY_RUN
puts "🧪 DOCUMENT VALIDATION ENABLED" if VALIDATE_DOCS

client_source = Elasticsearch::Client.new(
  url: CLUSTER_SOURCE_URL,
  user: SOURCE_ES_USER,
  password: SOURCE_ES_PASSWORD,
  transport_options: {
    ssl: { verify: false }
  },
  log: false
)

client_target = Elasticsearch::Client.new(
  url: CLUSTER_TARGET_URL,
  user: TARGET_ES_USER,
  password: TARGET_ES_PASSWORD,
  transport_options: {
    ssl: { verify: false }
  },
  log: false
)

# ============================================================
# STEP 1: FETCH SETTINGS + MAPPING FROM SOURCE
# ============================================================

puts "📥 Fetching settings and mapping from source index: #{SOURCE_INDEX}"

settings_response = client_source.indices.get_settings(index: SOURCE_INDEX)
mapping_response  = client_source.indices.get_mapping(index: SOURCE_INDEX)

source_settings =
  settings_response
    .dig(SOURCE_INDEX, 'settings', 'index')

source_mapping =
  mapping_response
    .dig(SOURCE_INDEX, 'mappings')

raise "❌ Source index settings not found" if source_settings.nil?
raise "❌ Source index mapping not found"  if source_mapping.nil?

# ============================================================
# STEP 2: CLEAN SETTINGS (COPY‑SAFE)
# ============================================================

ILLEGAL_SETTINGS = %w[
  uuid
  creation_date
  provided_name
  version
]

clean_settings =
  source_settings.reject { |k, _| ILLEGAL_SETTINGS.include?(k) }

# ============================================================
# STEP 3: FORCE STRICT MAPPING
# ============================================================

strict_mapping = source_mapping.merge('dynamic' => 'strict')

def enforce_strict_dynamic(properties)
  return unless properties.is_a?(Hash)

  properties.each do |_, field|
    if field['type'] == 'object' && field['properties']
      field['dynamic'] = 'strict'
      enforce_strict_dynamic(field['properties'])
    end
  end
end

enforce_strict_dynamic(strict_mapping['properties'])

puts "✅ Strict mapping constructed"

# ============================================================
# STEP 4: VALIDATE TEST DOCS USING EXISTING INGEST PIPELINE
# ============================================================

if VALIDATE_DOCS
  raise "❌ VALIDATION_DOCS_PATH not found: #{VALIDATION_DOCS_PATH}" \
    unless Dir.exist?(VALIDATION_DOCS_PATH)

  raise "❌ TARGET_ES_PIPELINE_ID must be provided when VALIDATE_DOCS=true" \
    if TARGET_ES_PIPELINE_ID.nil? || TARGET_ES_PIPELINE_ID.empty?

  temp_index = "strict_validation_#{SecureRandom.hex(6)}"

  puts "🧪 Creating temporary strict validation index: #{temp_index}"
  puts "🔧 Using existing ingest pipeline: #{TARGET_ES_PIPELINE_ID}"

  client_target.indices.create(
    index: temp_index,
    body: {
      settings: clean_settings,
      mappings: strict_mapping
    }
  )

  errors = []

  Dir.glob(File.join(VALIDATION_DOCS_PATH, '*.json')).each do |file|
    doc = JSON.parse(File.read(file))

    begin
      client_target.index(
        index: temp_index,
        pipeline: TARGET_ES_PIPELINE_ID,
        body: doc
      )
    rescue => e
      errors << {
        file: File.basename(file),
        error: e.message
      }
    end
  end

  puts "🧹 Deleting temporary validation index"
  client_target.indices.delete(index: temp_index)

  if errors.any?
    puts
    puts "❌ STRICT SCHEMA VALIDATION FAILED"
    puts "--------------------------------"
    errors.first(10).each do |err|
      puts "• #{err[:file]} → #{err[:error]}"
    end
    puts
    puts "⚠️  #{errors.size} document(s) failed strict mapping validation"
    exit 1
  else
    puts "✅ All test documents passed strict mapping validation"
  end
end

# ============================================================
# STEP 5: DRY‑RUN EXIT
# ============================================================

target_exists = client_target.indices.exists?(index: PHYSICAL_TARGET_INDEX)

if DRY_RUN
  puts
  puts "🧪 DRY‑RUN SUMMARY"
  puts "------------------"
  puts "• Source index  : #{SOURCE_INDEX}"
  puts "• Target index  : #{PHYSICAL_TARGET_INDEX}"
  puts "• Target exists : #{target_exists}"
  puts "• Action        : #{target_exists ? 'DELETE + CREATE' : 'CREATE'}"
  puts "• Mapping mode  : STRICT"
  puts "• Settings copy : YES (incl. analyzers)"
  puts
  puts "✅ Dry‑run complete — no changes applied"
  exit 0
end


# ============================================================
# STEP 6: CREATE TARGET INDEX + DATE-SUFFIXED ALIAS
# ============================================================

if client_target.indices.exists?(index: PHYSICAL_TARGET_INDEX)
  puts "🗑️  Deleting existing physical index: #{PHYSICAL_TARGET_INDEX}"
  client_target.indices.delete(index: PHYSICAL_TARGET_INDEX)
end

if client_target.indices.exists_alias?(name: TARGET_ALIAS)
  puts "🗑️  Removing existing alias: #{TARGET_ALIAS}"
  client_target.indices.delete_alias(index: '_all', name: TARGET_ALIAS)
end

puts "🚀 Creating physical index with dated alias: #{TARGET_ALIAS}"

client_target.indices.create(
  index: PHYSICAL_TARGET_INDEX,
  body: {
    settings: clean_settings,
    mappings: strict_mapping,
    aliases: {
      TARGET_ALIAS => {}
    }
  }
)

puts "✅ Physical index created: #{PHYSICAL_TARGET_INDEX}"
puts "✅ Alias created: #{TARGET_ALIAS} → #{PHYSICAL_TARGET_INDEX}"
