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
        
            # loop over the enrichment[:data] and check if it must be added to or replace the data in d
            # based in ["prov:wasAttributedTo"][@id] and ["prov:wasAssociatedFor"]["@id"]
            # The data model is 
            # "prov:wasAttributedTo" : [ 
            #   { 
            #        @id: 
            #        "prov:wasAssociatedFor" : [
            #          {
            #             @id:   
            #          }
            #        ]
            #    }
            # ]
            #
            # What to do with the new values in "prov:generated"
            # add to existing of replace ?
            # ToDo  enrichment[:data]["prov:wasAttributedTo"].nil?  ????
            # ==>  enrichment[:data].nil ==> Must "prov:wasAttributedTo" be deleted ?

            unless enrichment[:data].nil?
                enrichment[:data]["prov:wasAttributedTo"] = [enrichment[:data]["prov:wasAttributedTo"]] unless enrichment[:data]["prov:wasAttributedTo"].is_a?(Array)

                if d["prov:wasAttributedTo"]
                    d["prov:wasAttributedTo"] = [ d["prov:wasAttributedTo"]  ] unless d["prov:wasAttributedTo"].is_a?(Array)
                    enrichment[:data]["prov:wasAttributedTo"].each do |enrich_wasAttributedTo|
                        enrich_wasAttributedTo["prov:wasAssociatedFor"].each do |enrich_wasAssociatedFor|
                            enrichment_processed = false 
                            d["prov:wasAttributedTo"].map! { |prov_wasattributedto|
                                if prov_wasattributedto["@id"] == enrich_wasAttributedTo["@id"]
                                    prov_wasattributedto["prov:wasAssociatedFor"].compact!
                                    prov_wasattributedto["prov:wasAssociatedFor"].map! {  |prov_wasssociatedfor| 
                                            if prov_wasssociatedfor["@id"] == enrich_wasAssociatedFor["@id"]
                                                enrichment_processed = true
                                                enrich_wasAssociatedFor["prov:generated"] = [ enrich_wasAssociatedFor["prov:generated"] ] unless enrich_wasAssociatedFor["prov:generated"].is_a?(Array)
                                                enrich_wasAssociatedFor["prov:generated"] << prov_wasssociatedfor["prov:generated"] 
                                                enrich_wasAssociatedFor["prov:generated"].flatten!
                                                prov_wasssociatedfor["prov:generated"]  = enrich_wasAssociatedFor["prov:generated"].uniq { |h| deep_sort_hash(h).to_json }
                                            end
                                            prov_wasssociatedfor
                                    }
                                    unless enrichment_processed
                                        enrichment_processed = true
                                        prov_wasattributedto["prov:wasAssociatedFor"] << enrich_wasAssociatedFor
                                    end
                                end
                                prov_wasattributedto
                            }

                            d["prov:wasAttributedTo"].compact!

                            unless enrichment_processed
                                enrichment_processed = true
                                d["prov:wasAttributedTo"] << enrich_wasAttributedTo
                            end
                        end
                    end
                else
                    d["prov:wasAttributedTo"] = enrichment[:data]["prov:wasAttributedTo"]
                end
            end

            # d["prov:wasAttributedTo"].each do | wasAttributedTo | 
            #     pp "dededededededededededededededededededehhhhhhhh"
            #     pp wasAttributedTo["prov:wasAssociatedFor"][0]["@id"]
            #     pp wasAttributedTo["prov:wasAssociatedFor"][0].is_a?(Hash)
            #     if wasAttributedTo["prov:wasAssociatedFor"][0].is_a?(Hash)
            #         if wasAttributedTo["prov:wasAssociatedFor"][0]["prov:generated"].is_a?(Array)
            #             generated =  wasAttributedTo["prov:wasAssociatedFor"][0]["prov:generated"]
            #             pp generated[0]["result"].size
            #         end
            #     else
            #         pp wasAttributedTo["prov:wasAssociatedFor"]
            #     end
            # end

            d
        } ] }
    }
}
