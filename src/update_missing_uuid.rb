#encoding: UTF-8
$LOAD_PATH << '.' << './lib'

require 'logger'
require_relative './lib/loader'

@logger = Logger.new(STDOUT)

begin
  #Create a loader object.
  loader = Loader.new()
  config = loader.config()

  loader.logger.info "use utils for es_cluster : #{loader.es_cluster}"

  loader.check_elastic()

  filename = "/records/missing_uuids.txt"
  filename_not_in_icandid = "/records/not_in_elastic.txt"
  filename_wrong_uuids = "/records/wrong_uuids.txt"
  counter = 0

  q='
 {
  "query": {
    "bool": {
      "must": [
        { "term": {
          "@uuid": {
            "value": "{{uuid}}"
          }
        }}
      ]
    }
  }
}
'
  # Clear the file if it exists
  not_in_icandid_file  = File.open(filename_not_in_icandid, "w") # Opening in "w" mode clears the file

  # Clear the file if it exists
  wrong_uuids_file = File.open(filename_wrong_uuids, "a") 


  pp "wrong_uuids_file #{filename_wrong_uuids}"
  wrong_uuids = File.readlines(filename_wrong_uuids)&.map(&:chomp)&.map { |line| line.split(":", 2) } || []
  

  
  wrong_uuids = wrong_uuids.each_with_object(Hash.new { |h, k| h[k] = [] }) do |(key, value), hash|
    hash[key] << value
  end

  wrong_uuids_keys = wrong_uuids.keys
  
  File.open("#{filename}", "r") do |f|
    f.each_line do |line|
      uuid = line.strip!
      next if line.empty?
      @logger.info "Processing UUID: #{uuid}"

      # Search the matches
      if wrong_uuids_keys.include?(uuid)
        @logger.warn  "Found matches for uuid in wrong_uuids: #{uuid} #{ wrong_uuids[uuid] } }"
        next
      end


      records = loader.search(
        index: config[:es_index],
        body: q.gsub("{{uuid}}", uuid)
        # , scroll: '1m'
      )
      if records['hits']['total']['value'] == 0
        @logger.warn "No records found for UUID: #{uuid}"
        exists_url = "https://services.libis.be/uuid/exists/#{uuid}"
        http = HTTP 
        response = http.get(exists_url)
        uuid_response = JSON.parse( response.body )

        # ids from ENA should be of format iCANDID_ena_[vrt|vtm]_[1,2]
        if uuid_response["from_uuid"] =~ /iCANDID_ena_[1,2]/
          pp "===========>>>>>>>> #{uuid_response["from_uuid"]} write to wrong_uuids_file"
          wrong_uuids_file.puts "#{uuid}:#{uuid_response["from_uuid"]}"
          # exit
          next
        end

        not_in_icandid_file.puts "#{uuid} : #{uuid_response["from_uuid"]}"
        next
      end
      @logger.info "Found #{records['hits']['total']['value']} records for UUID: #{uuid}"
      hit = records.dig('hits', 'hits')[0]
      unless hit.nil?
        icandid_id = hit['_id']
        pp "Updating record with ID: #{icandid_id} for UUID: #{uuid}"

exit        
        icandid_url = "https://icandid.libis.be/_/#{uuid}"
        exists_url = "https://services.libis.be/uuid/exists/#{uuid}"
        metadata_resolver = "https://services.libis.be/resolver/metadata/icandid"
        
        http = HTTP 
        response = http.get(icandid_url)
        if response.status == 200
          @logger.info "Record found: #{icandid_url} - Status: #{response.status}"
        end
        if response.status == 302
          @logger.info "Record redirect: #{icandid_url} - Status: #{response.status}"
        end
        if response.status == 404
          @logger.warn "Record not found: #{icandid_url} - Status: #{response.status}"
          
          response = http.get(exists_url)
          if response.status == 200
            @logger.info "#{uuid} exists"
            @logger.warn "#{uuid} must be added with #{metadata_resolver}"
            sleep 0.5

            uuid_response = JSON.parse( response.body )
            email = uuid_response["created"]["by"]
            owner =  uuid_response["created"]["product"]
            if email == "icandid_tech@libis.kuleuven.be" && owner == "icandid"
              body = {
                "set": "url",
                "work": [
                    {
                        "create": {
                            "id": uuid,
                            "owner": "icandid",
                            "email": "icandid_tech@libis.kuleuven.be",
                            "url": uuid_response["resolve"]["url"],
                            "alias": icandid_id
                        }
                    }
                ]
              } 
              # pp body
              metadata_resolver_url = "#{metadata_resolver}/#{uuid}"
              response = http.post(metadata_resolver_url, json: body)
              if response.status == 200 
                @logger.info "Record added to #{metadata_resolver_url}"
              else
                @logger.error "Failed to add record to resolver: #{metadata_resolver_url} - Status: #{response.status}"
                exit(1)
              end
            else
              @logger.warn "#{uuid} exists but not owned by icandid"
            end
              
          else
            @logger.error "#{uuid} does not exists: #{exists_url} - Status: #{response.status}"
            @logger.error "Record not found: #{icandid_url} - Status: #{response.status}"
          end
        end
        @logger.info "Record with permalink #{icandid_url}\n\n"

      end

    end
  end

rescue Exception => e
  @logger.error "Error in update_missing_uuid.rb: #{e.message}"
  @logger.error e.backtrace.join("\n")
  exit(1)
end
