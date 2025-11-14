#encoding: UTF-8
require 'data_collector'
require "iso639"

include DataCollector::Core

GOOGLE_AI_RULE_SET_v1_0 = {
    version: "1.0",

    rs_records: {
        records: { "@" => lambda { |d,o|  
            records = DataCollector::Output.new

            # d => the record from ES
            # o["enrichment"] the record (from disk) that will be added as enrichment

            rules_ng.run(GOOGLE_AI_RULE_SET_v1_0[:rs_record], d, records, o)
=begin
This was added if the enrichments should be part of a specific associatedMedia object instead of the entire record

            if records[:records].has_key?( "associatedMedia" ) 
                records[:records]["associatedMedia"] = [ records[:records]["associatedMedia"] ] unless records[:records]["associatedMedia"].is_a?(Array)
                records[:records]["associatedMedia"].map! { |a| 
                    a["hasPart"].map!{ |hp| 
                        if Regexp.new( o["enrichment"]["@id"].split('_')[1..].join('_')  ) =~ hp["identifier"]["value"] 

                            o["enrichment_is_based_on"] = ["url"]

                            records_hp = DataCollector::Output.new
                            rules_ng.run(GOOGLE_AI_RULE_SET_v1_0[:rs_record], hp, records_hp, o)
                            hp = records_hp[:records]
                        end
                        hp
                    };
                    a
                } 
            end
=end
            records[:records]
        } }
    },
    rs_record: {
        records: { "$" => [ lambda { |d,o|  
            enrichment = DataCollector::Output.new

            rules_ng.run( o[:rule_set].constantize[:rs_data_enrichment], o[:enrichment]["_source"], enrichment, o)
                        
     
            d = process_enrichment(enrichment, d)
            return d

        } ] }
    }
}
