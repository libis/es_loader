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
API_KEY = 'check your profile in iCANDID' 

require 'json'


describe 'Search index API with Faraday - Parallel Payloads' do
  api_key = ENV['API_KEY'] || API_KEY
  index_array = (ENV['INDEX'] || 'any').split(',')

  conn = Faraday.new(url: SEARCH_URL) do |f|
    f.request :json
    f.response :json, content_type: /\bjson$/
    f.adapter Faraday.default_adapter
  end

  def search(conn, api_key, payload)
    conn.post('/icandid/_search') do |req|
      req.headers['APIKEY'] = api_key
      req.headers['Content-Type'] = 'application/json'
      req.body = payload
    end
  end

  # Load payloads immediately 
  all_payloads = []
  if ENV['INDEX'].nil?
    queries_dir = File.expand_path("./support/", __dir__)
    Dir.entries(queries_dir).select { |f| f.match(/^payloads_.*_queries\.json$/) }.each do |file|
      full_path = File.join(queries_dir, file)
      all_payloads.concat JSON.parse(File.read(full_path))
    end
  else
    index_array.each do |index|
      payload_file = File.expand_path("./support/payloads_#{index}_queries.json", __dir__)
      unless File.file?(payload_file)
        raise "#{payload_file} does not exists !!!!!!!!!"
        payload_file = File.expand_path('./support/payloads_queries.json', __dir__)
      end
      all_payloads.concat JSON.parse(File.read(payload_file))
    end
  end
  # Parallel slicing
  worker_id     = (ENV['TEST_ENV_NUMBER'] || '0').to_i
  total_workers = ENV['PARALLEL_TEST_GROUPS']&.to_i || 4
  #payloads_for_this_worker = all_payloads.each_slice((all_payloads.size / total_workers.to_f).ceil).to_a[worker_id]

  payloads_for_this_worker = all_payloads

  payloads_for_this_worker.each do |payload|
    it "returns results for query: '#{payload['query']}'" do
      response = search(conn, api_key, payload)
      expect(response.status).to eq(200)
      expect(response.body['hits']["hits"]).to be_an(Array)
      expect(response.body['hits']["total"]["value"]).to be > 0
    end
  end
end