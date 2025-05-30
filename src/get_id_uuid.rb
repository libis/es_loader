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
  

  filename = "records_ids_from_#{ config[:es_index] }"
  counter = 0

  filename = "#{filename}_#{Time.new.strftime("%Y%m%d_%H%M")}.json"

  q='
  { 
      "_source": ["@id", "@uuid"],
      "size": 50,
      "query" : { 
          "bool" : {
            "must": [ {
                  "range": {
                    "updatetime": {
                      "gte": "now-2d/d",
                      "lte": "now/d"
                    }
                  }
                }]
          } 
      },
      "stored_fields": [],
      "track_total_hits":true
  }
  '


  records = loader.search(index: config[:es_index], body: q, scroll: '1m')
  hits = records.dig('hits', 'hits')

  all_hits = {}
  File.open("/records/#{filename}", "wb") do |f|
      while !hits.empty? && counter < 1
        hits = records.dig('hits', 'hits')
          break if hits.empty?
          hits.each do |hit|
            if  all_hits.has_key? hit["_source"]["@uuid"] 
              all_hits[ hit["_source"]["@uuid"] ] < hit["_source"]["@id"] 
            else
              all_hits[ hit["_source"]["@uuid"] ] = [ hit["_source"]["@id"] ]
            end

            f.puts "#{hit["_id"]}, #{hit["_source"]["@id"]}, #{hit["_source"]["@uuid"]}"
          end

          counter = counter + 1
          print "."
          $stdout.flush 
                   
            records = loader.scroll(
            index: config[:es_index], 
            body: { :scroll_id => records['_scroll_id'] }, 
            scroll: '1m'
          )

          hits = records.dig('hits', 'hits')
      end
      puts "."
  end

  filename = "#{filename}_all_#{Time.new.strftime("%Y%m%d_%H%M")}.json"

  File.open("/records/#{filename}", 'w') do |f|
    f.puts(all_hits.to_json)
  end

  puts "/records/#{filename}"
  q = { :scroll_id => records["_scroll_id"]}.to_json

rescue Exception => e
  @logger.error "Error in get_id_uuid.rb: #{e.message}"
  @logger.error e.backtrace.join("\n")
  exit(1)
end


















