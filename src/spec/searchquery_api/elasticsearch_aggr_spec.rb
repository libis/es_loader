require 'uri'
require 'json'
require 'faraday'
require 'faraday_middleware'


#########################################
# $ => API_KEY=your_key rake api_tests  #
#########################################


# SEARCH_URL = 'https://icandid.t.libis.be'
# SEARCH_URL = 'http://localhost:9292'
SEARCH_URL = 'http://host.docker.internal:9292'
ES_INDEX = 'icandid'
API_KEY = 'check your profile in iCANDID' 

require 'json'


describe 'Aggregations API with Faraday - Parallel Payloads' do
  api_key = ENV['API_KEY'] || API_KEY

  conn = Faraday.new(url: SEARCH_URL) do |f|
    f.request :json
    f.response :json, content_type: /\bjson$/
    f.adapter Faraday.default_adapter
  end

  def search(conn, api_key, payload)
    conn.post("/#{ES_INDEX}/_search") do |req|
      req.headers['APIKEY'] = api_key
      req.headers['Content-Type'] = 'application/json'
      req.body = payload
    end
  end


  def eval_buckets(buckets)
    buckets.each do |bucket|
      if bucket.has_key?('TEST_AGGR')
        pp 
        expect(bucket['TEST_AGGR']["buckets"].size).to be > 0
      end
    end
  end


  def eval_aggregations(aggregations)
    if aggregations.has_key?('TEST_AGGR')
      if aggregations['TEST_AGGR']["buckets"].is_a?(Array)
        
        expect(aggregations['TEST_AGGR']["buckets"].size).to be > 0
        expect(aggregations['TEST_AGGR']["buckets"][0]['doc_count']).to be > 0
        aggregations['TEST_AGGR']["buckets"].each do |bucket|
          if bucket.has_key?("TEST_AGGR")
            eval_aggregations(bucket)
          end
        end
      end 
      if aggregations['TEST_AGGR']["buckets"].is_a?(Hash)
        if aggregations['TEST_AGGR']["buckets"].has_key?("TEST_AGGR")
          eval_aggregations(aggregations['TEST_AGGR']["buckets"])
        end
      end
      if aggregations['TEST_AGGR'].has_key?('TEST_AGGR')
        expect(aggregations['TEST_AGGR']["doc_count"]).to be > 0
        eval_aggregations(aggregations['TEST_AGGR']['TEST_AGGR'])
      end
    end
  end
  # Load payloads immediately 
  all_payloads = []

  queries_dir = File.expand_path("./support/", __dir__)
  Dir.entries(queries_dir).select { |f| f.match(/^payloads_.*_aggs\.json$/) }.each do |file|

    full_path = File.join(queries_dir, file)
    pp "========> #{full_path}"
    all_payloads.concat JSON.parse(File.read(full_path))
  end
  # Parallel slicing
  worker_id     = (ENV['TEST_ENV_NUMBER'] || '0').to_i
  total_workers = ENV['PARALLEL_TEST_GROUPS']&.to_i || 4
  #payloads_for_this_worker = all_payloads.each_slice((all_payloads.size / total_workers.to_f).ceil).to_a[worker_id]

  payloads_for_this_worker = all_payloads

  payloads_for_this_worker.each do |payload|
    it "returns results for query: '#{payload['query']}'" do
      response = search(conn, api_key, payload)

      expect(response.body['aggregations']['TEST_AGGR']).to be_an(Hash)
      
      aggregations = response.body['aggregations']

      eval_aggregations(aggregations)

    end
  end
end