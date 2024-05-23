#encoding: UTF-8
require 'data_collector'
require "iso639"

include DataCollector::Core

GOOGLE_AI_RULE_SET_v1_0 = {
    version: "1.0",

    rs_records: {
        records: { "@" => lambda { |d,o|  
            records = DataCollector::Output.new
            rules_ng.run(GOOGLE_AI_RULE_SET_v1_0[:rs_record], d, records, o)

            records[:records]["associatedMedia"].map! { |a| 
                a["hasPart"].map!{ |hp| 
                    if Regexp.new( o["enrichment"]["@id"].split('_')[1..].join('_')  ) =~ hp["identifier"]["value"] 

                        o["itemListElement"] = ["url"]

                        records_hp = DataCollector::Output.new
                        rules_ng.run(GOOGLE_AI_RULE_SET_v1_0[:rs_record], hp, records_hp, o)
                        hp = records_hp[:records]
                    end
                    hp
                };
                a
            } 

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

            enrichment[:data]["prov:wasAttributedTo"] = [enrichment[:data]["prov:wasAttributedTo"]] unless enrichment[:data]["prov:wasAttributedTo"].is_a?(Array)

            if d["prov:wasAttributedTo"]
                d["prov:wasAttributedTo"] = [ d["prov:wasAttributedTo"]  ] unless d["prov:wasAttributedTo"].is_a?(Array)

                enrichment[:data]["prov:wasAttributedTo"].each do |enrich_wasAttributedTo|
                    enrich_wasAttributedTo["prov:wasAssociatedFor"].each do |enrich_wasAssociatedFor|
                        enrichment_processed = false 

                        d["prov:wasAttributedTo"].map! { |prov_wasattributerto|

                            if prov_wasattributerto["@id"] == enrich_wasAttributedTo["@id"]

                                prov_wasattributerto["prov:wasAssociatedFor"].map! {  |prov_wasssociatedfor| 

                                    if prov_wasssociatedfor["@id"] == enrich_wasAssociatedFor["@id"]

                                        enrich_wasAssociatedFor["prov:generated"].concat prov_wasssociatedfor["prov:generated"]
                                        enrich_wasAssociatedFor["prov:generated"].uniq!

                                        enrichment_processed = true
                                       
                                        prov_wasssociatedfor = enrich_wasAssociatedFor
                                    end
                                    prov_wasssociatedfor
                                }
                                
                                unless enrichment_processed
                                    enrichment_processed = true
                                    prov_wasattributerto["prov:wasAssociatedFor"] << enrich_wasAssociatedFor
                                end
                            end
                            prov_wasattributerto 
                        }

                        unless enrichment_processed
                            enrichment_processed = true
                            d["prov:wasAttributedTo"] << enrich_wasAttributedTo
                        end

                    end
                end
            else
                d["prov:wasAttributedTo"] = enrichment[:data]["prov:wasAttributedTo"]
            end

            d
        } ] }
    }
}
