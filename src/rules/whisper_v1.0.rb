#encoding: UTF-8
require 'data_collector'
require "iso639"

include DataCollector::Core

WHISPER_RULE_SET_v1_0 = {
    version: "1.0",

    rs_records: {
        records: { "@" => lambda { |d,o|  
            records = DataCollector::Output.new
            # d => the record from ES
            # o["enrichment"] the record (from disk) that will be added as enrichment
            rules_ng.run(WHISPER_RULE_SET_v1_0[:rs_record], d, records, o)

            records[:records]
        } }
    },
    rs_record: {
        records: { "$" => [ lambda { |d,o|  
            enrichment = DataCollector::Output.new
            rules_ng.run( o[:rule_set].constantize[:rs_data_enrichment], o[:enrichment]["_source"], enrichment, o)
            d = process_enrichment(enrichment, d)

            # d["prov:wasAttributedTo"].each do | wasAttributedTo | 
            #     pp "dededededededededededededededededededehhhhhhhh"
            #     pp wasAttributedTo["prov:wasAssociatedFor"][0]["@id"]
            #     pp wasAttributedTo["prov:wasAssociatedFor"][0].is_a?(Hash)
            #     if wasAttributedTo["prov:wasAssociatedFor"][0].is_a?(Hash)
            #         if wasAttributedTo["prov:wasAssociatedFor"][0]["prov:generated"].is_a?(Array)
            #             pp wasAttributedTo["prov:wasAssociatedFor"][0]["prov:generated"][0].keys
            #         end
            #          if wasAttributedTo["prov:wasAssociatedFor"][0]["prov:generated"].is_a?(Hash)
            #             pp wasAttributedTo["prov:wasAssociatedFor"][0]["prov:generated"].keys
            #         end
            #     else
            #         pp wasAttributedTo["prov:wasAssociatedFor"]
            #     end
            #     pp "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeend"
            # end
            
            return d
        } ] }
    }
}
