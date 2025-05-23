#encoding: UTF-8
$LOAD_PATH << '.' << './lib'

require 'net/smtp'
require "http"
require 'logger'
require 'fileutils'
require 'json'
require 'pp'
require 'date'

#def config
#    @config ||= ConfigFile
#end

ONLY_ONE_VALUE_ALLOWED = ["name","headline","articleBody","description"]
CONTEXT = {
              "@vocab" => "https://schema.org/",
              "prov:wasAssociatedFor" => {
                "@reverse" => "prov:wasAssociatedWith"
              },
              # "ownedBy": { "@reverse": "https://schema.org/owns" },
              "@language" => "nl-Latn",
              "prov" => "https://www.w3.org/ns/prov#"
          }
          
UUID_URL_PREFIX = "https://icandid.libis.be/_/"


def default_options
  { 
    :config_file         => "es_loader_conf.yml",
    :log                 => "es_loader.log",
    :last_run_updates    => '2000-01-01T11:11:11+01:00',
    :max_records_per_file => 300,
    :full_reload         => false,
    :load_type           => "update",
    :record_dirs_to_load => nil,
    :record_pattern      => nil,
    :es_url              => nil,
    :es_version          => nil,
    :es_index            => nil,
    :es_pipeline_id      => nil,
    :import_mappings     => "/elastic/mappings.json",
    :import_settings     => "/elastic/settings.json",
    :import_pipeline     => "/elastic/pipeline.json"
  }
end

def  checkLangauge( jsondata, field_path, lang )
    fields = field_path.split('.')
    if fields.first == fields.last
        jsondata[ field_path ] = checkFieldLangauge( jsondata[ field_path ] , lang  )
    else
        field =  fields.shift 
        field_path =  fields.join('.') 
        data = jsondata[field]
        if ! data.nil? 
            if data.is_a?(Array)
                #data.map! { |d| checkLangauge( d, field_path, lang ) }
                data.each { |d| 
                    checkLangauge( d, field_path, lang )
                }
            else
                checkLangauge( data, field_path, lang )
            end
        end
    end
end

def  checkFieldLangauge( field, lang  )
    if field.is_a? String 
        x = field
        field = { "@value" => x , "@language" => "#{ lang }" }
    else
        if field.is_a?(Array)
            field =  field.map { |x| 
                if x.is_a? String 
                { "@value" => x , "@language" => "#{ lang }" } 
                else x 
                end
            }
        end
    end
    field
end

def  checkPersonLangauge( person, lang  )
    if  person['name'].is_a? String 
        person['name']  = { "@value" => person['name'] , "@language" => "#{ lang }" } 
    end
    if  person['familyName'].is_a? String 
        person['familyName']  = { "@value" => person['familyName'] , "@language" => "#{ lang }" } 
    end
    person
end


def  parseDate( date, c_int  )
    r = nil
    date.upcase.each_char do |c| 
        if ["X", "U", "?"].include?(c) 
            r = ( (r.nil?) ? c_int  : r + c_int  )
        else
            r = ( (r.nil?) ? c : r + c )
        end
    end
    return r
end

def to_jsonfile (jsondata, jsonfile, records_dir)
    file_name = "#{records_dir}/#{jsonfile}_#{Time.now.to_i}_#{rand(1000)}.json"
    File.open(file_name, 'wb') do |f|
      f.puts jsondata.to_json
    end
rescue Exception => e
    raise "unable to save to jsonfile: #{e.message}"
end

def convert_hash_keys(value)
    case value
    when Array
        value.map { |v| convert_hash_keys(v) }
    when Hash
        Hash[value.map { |k, v| [ k.to_s, convert_hash_keys(v)] } ]
    else
        value
    end
end

def create_record(jsondata)

  if jsondata['@context'].nil?
    raise "jsondata['@context'] is nil !"
  end

  lang = "nl-Latn"

  if jsondata['@context'].is_a? String
    jsondata['@context'] = [ jsondata['@context'] , "@language": "nl-Latn" ]
  end
 
  if jsondata["@context"].is_a?(Array)
    lang = jsondata["@context"].map { |c| c["@language"] }.compact.first
  end

  if jsondata["@context"].is_a?(Hash)
    lang = jsondata["@context"]["@language"] 
  end

  jsondata["@context"] = CONTEXT
  jsondata["@context"]["@language"] = lang
 
  fromprocessingtime = Time.now.strftime("%Y-%m-%d %H:%M:%S")
  jsondata['processingtime'] = "#{ Time.now.strftime("%Y-%m-%d %H:%M:%S") }"
  
  unless jsondata["author"].nil? 
      if jsondata["creator"].nil?
          jsondata["creator"] = jsondata["author"]
          jsondata.delete("author")
      end
  end

  unless  jsondata["citation"].nil?
      jsondata["citation"]["@context"] = CONTEXT
      unless  jsondata["citation"]["citation"].nil?
          jsondata["citation"].delete("citation") 
      end 
  end

  checkLangauge( jsondata, 'name', lang )
  checkLangauge( jsondata, 'text', lang )
  checkLangauge( jsondata, 'keywords', lang )

  if jsondata['datePublished'].is_a?(Array)
      datePublished =  jsondata['datePublished'][0]
  else
      datePublished = jsondata['datePublished'] 
  end 

  unless datePublished.nil?

    begin
      date_format = "%Y-%m-%d %H:%M:%S"
      # replace character U and X with 0 and 9
      # yyyy-yyyy
      if (datePublished =~ /^.*([0-9UX?]{4}-[0-9UX?]{4}).*$/)
          datePublished = datePublished[/^.*([0-9UX?]{4}-[0-9UX?]{4}).*$/,1]
      end
      if (datePublished =~ /^[0-9UX?]{4}-[0-9UX?]{4}$/) 
          fromyear = parseDate( datePublished[0, 4],"0");
          tillyear = parseDate( datePublished[5, 9],"9");
      end
      
      # yyyy
      if (datePublished =~ /^[0-9UX?]{4}$/) 
          fromyear = parseDate( datePublished[0, 4],"0");
          tillyear = parseDate( datePublished[0, 4],"9");
      end

      if !fromyear.nil? && !tillyear.nil?
        jsondata['_datePublished'] = [ "#{ (DateTime.parse( "#{fromyear}-01-01 00:00:00" )).strftime(date_format) }","#{ (DateTime.parse( "#{tillyear}-12-31 23:59:59" )).strftime(date_format) }" ]
        datePublished_time_frame = ["gte": "0000"];
        datePublished_time_frame = {
            "gte" => fromyear,
            "lte" => tillyear  
        }
      else
        jsondata['_datePublished'] = "#{ (DateTime.parse( datePublished )).strftime(date_format) }" 
        datePublished_time_frame = {
          "gte" => "#{ (DateTime.parse( datePublished )).strftime(date_format) }",
          "lte" => "#{ (DateTime.parse( datePublished )).strftime(date_format) }"  
        }
      end
      
      jsondata['datePublished_time_frame'] = datePublished_time_frame

    rescue StandardError => e
      pp "ERROR Parsing datePublished #{e}  [ #{datePublished} ] in ( #{ jsondata['@id']  } ) "
      jsondata['_datePublished'] = nil
      jsondata['datePublished_time_frame'] = nil

      jsondata.compact
    end

  end

  if jsondata['dateCreated'].is_a?(Array)
      dateCreated =  jsondata['dateCreated'][0]
  else
      dateCreated = jsondata['dateCreated'] 
  end 

  unless dateCreated.nil?
      if (dateCreated =~ /^.*([0-9UX?]{4}-[0-9UX?]{4}).*$/)
          dateCreated = dateCreated[/^.*([0-9UX?]{4}-[0-9UX?]{4}).*$/,1]
      end

      if (dateCreated =~ /^[0-9UX?]{4}-[0-9UX?]{4}$/) 
          fromyear = parseDate( dateCreated[0, 4],"0");
          tillyear = parseDate( dateCreated[5, 9],"9");
      end

      if (dateCreated =~ /^[0-9UX?]{4}$/) 
          fromyear = parseDate( dateCreated[0, 4],"0");
          tillyear = parseDate( dateCreated[0, 4],"9");
      end

      if !fromyear.nil? && !tillyear.nil?
          dateCreated_time_frame = ["gte": "0000"];
          dateCreated_time_frame = {
              "gte" => fromyear ,
              "lte" => tillyear  
          }
      end
      jsondata['dateCreated_time_frame'] = dateCreated_time_frame
      
      begin
        jsondata['dateCreated'] = "#{ (DateTime.parse( dateCreated )).strftime(date_format) }"  
      rescue StandardError => e
        pp "ERROR Parsing dateCreated #{e}  [ #{dateCreated} ] "
        jsondata['dateCreated'] = nil
        jsondata['dateCreated_time_frame'] = nil
      end

  end

  tillprocessingtime = Time.now.strftime("%Y-%m-%d %H:%M:%S")
  processingtime_time_frame = {
      "gte" => fromprocessingtime ,
      "lte" => tillprocessingtime 
  }
  jsondata['processingtime_time_frame'] = processingtime_time_frame

  jsondata.reject!{ |j| j.empty? || j.nil? }
  jsondata.reject!{ |k,v| v.nil? || v.to_s.empty? }

  #pp jsondata.keys
    
  jsondata = add_ids( jsondata )
  jsondata = add_uuid( jsondata )

  #pp jsondata["actor"]
  
  return jsondata

end

def preprocess_records(files_to_process)
    data_array = []
    files_to_process.each do |jsonfile|
        data_array << JSON.parse( File.read("#{jsonfile}") )
    end
    merge_json(data_array)
end

def merge_json(data_array)
    data = data_array.inject do |outputdata, data| 
        if (outputdata.is_a?(Hash) && data.is_a?(Hash) && outputdata["@id"] != data["@id"] )
          outputdata = [data, outputdata]
        elsif (outputdata.is_a?(Array) && data.is_a?(Hash) && !outputdata.map{ |k| k["@id"] }.include?(data["@id"]) )
          outputdata << data
        elsif (outputdata.is_a?(Array) && data.is_a?(Hash) && outputdata.map{ |k| k["@id"] }.include?(data["@id"]) )
            outputdata.map do |d|
              if d["@id"] == data["@id"]
                merge_json([d,data])
              else
                d
              end
            end
        else
          outputdata.merge!(data) do |key, v1, v2|
            #puts "inspect v1 #{v1.inspect}"
            #puts "inspect v2 #{v2.inspect}"
            if !v1.nil? && v2.nil?
              v1
            elsif v1.nil? && !v2.nil?
              v2
            elsif v1 == v2
              # puts "equal #{key}"
              v1
            else
              if ONLY_ONE_VALUE_ALLOWED.include?(key)
                puts "inspect key #{key.inspect}"
                v2
              elsif v1.is_a?(Array)
                if v2.is_a?(Array)
                  (v1 + v2).uniq
                elsif v2.is_a?(Hash)
                  merge_json([v1,v2])
                elsif v2.is_a?(String)
                    v1 << v2
                    v1.uniq
                else
                  raise "Can't merge a #{v2.inspect} to a hash!"
                end
              elsif v1.is_a?(Hash)
                if v2.is_a?(Array)
                  if v2.all? { |h| h.is_a?(Hash) }
                    v2 << v1
                    v2.uniq
                  else
                    raise "Can't merge a #{v2.inspect} to a hash!"
                  end
                elsif v2.is_a?(Hash)
                    merge_json([v1,v2])
                else
                  raise "Can't merge a #{v2.inspect} to a hash!"
                end
              else
                if v2.is_a?(String) || v2.is_a?(Integer)
                  if  key === "@value"
                    [v1,v2].uniq.max_by(&:length)
                  else
                    [v1,v2].uniq
                  end
                elsif v2.is_a?(Array)
                  v2 << v1
                  v2.uniq!
                  if  key === "@value"
                    v2.uniqBeginString.max_by(&:length)
                  else
                    v2.uniqBeginString
                  end
                elsif v2.is_a?(Hash)
                  if v2.has_key?("@value") && v2.has_key?("@language")
                    if v1.is_a?(String) 
                      raise "COMP v1 (string) #{v1} \n v2@value #{v2["@value"]} )"
                    else
                      raise "COMP v1 #{v1} \n v2@value #{v2["@value"]} )"
                    end
                  else
                    raise "Can't merge a #{v2.inspect} to a hash! (v1 string? class #{v1.class} )"
                  end
                else
                  
                  puts "---------------------------"
                  puts "v1"
                  pp v1
                  puts "---------------------------"
                  puts "v2"
                  pp v2
                  raise "Can't merge a #{v2.inspect} to a hash!"
                end
              end
            end
        end
      end
      outputdata
    end
    convert_hash_keys(data)
end


def add_ids( data, id = nil)
  property_list = [ "name", "roleName", "characterName",  "url", "embedUrl" ]
  excluded_types_list = [ "InteractionCounter" ]
  if id.nil?
    id = data["@id"]
  end
  if data.is_a?(Hash)
    data.map { |k, d|
      [ k, add_ids(d, id) ] 
    }.to_h
    if !data["@type"].nil? && data["@id"].nil? 
      unless excluded_types_list.include?(data["@type"])
        property_list.map! { |p| data[p] }
        val_to_md5_hash = property_list.compact.first
        if val_to_md5_hash.nil?
          pp data
          exit
        end
        if val_to_md5_hash.is_a?(Array)
          val_to_md5_hash = val_to_md5_hash.sort.first
        end
        # pp "#{id}_#{data["@type"].upcase}_#{Digest::MD5.hexdigest(val_to_md5_hash)}"
        data["@id"] = "#{id}_#{data["@type"].upcase}_#{ Digest::MD5.hexdigest(val_to_md5_hash) }"
      end
    end
  end
  if data.is_a?(Array)
    data.map! do |d|
      add_ids( d, id)
    end
  end
  data
end

def add_uuid( data)
 
  unless data["@id"].nil?
   
    uuid_url = "#{@config[:uuid_config][:url]}/#{data["@id"]}?by=#{@config[:uuid_config][:by]}&for=#{@config[:uuid_config][:for]}&resolvable=#{@config[:uuid_config][:resolvable]}"
    uuid_generator_response = HTTP.get(uuid_url)

    if uuid_generator_response.status == 201
      uuid = uuid_generator_response.parse
    end
    if uuid_generator_response.status == 400
      uuid = uuid_generator_response.parse["uuid"]
    end
    
    data["@uuid"] = uuid
    data["url"] = URI::join(UUID_URL_PREFIX, uuid).to_s

  end

  data
end

