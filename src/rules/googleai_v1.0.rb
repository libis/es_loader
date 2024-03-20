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
            records[:records]
        } }
    },
    rs_record: {
        records: { "$" => [ lambda { |d,o|  
            enrichment = DataCollector::Output.new
            rules_ng.run(GOOGLE_AI_RULE_SET_v1_0[:rs_data_enrichment], o[:enrichment]["_source"], enrichment, o)

            # loop over the enrichment[:data] and check if it must be added to are replace the data in d
            # based in ["prov:wasAttributedTo"][@id] and ["prov:wasAssociatedFor"]["@id"]

            if d["prov:wasAttributedTo"]
                enrichment[:data]["prov:wasAttributedTo"].each do |enrich_wasAttributedTo|
                    enrich_wasAttributedTo["prov:wasAssociatedFor"].each do |enrich_wasAssociatedFor|
                        enrichment_processed = false

                        d["prov:wasAttributedTo"].map! { |prov_wasattributerto|
                            if prov_wasattributerto["@id"] == enrich_wasAttributedTo["@id"]
                                prov_wasattributerto["prov:wasAssociatedFor"].map! {  |prov_wasssociatedfor| 
                                    if prov_wasssociatedfor["@id"] == enrich_wasAssociatedFor["@id"]
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
                            d["prov:wasAttributedTo"] << {"prov:wasAssociatedFor"=> enrich_wasAssociatedFor }
                        end
                    end
                end
            else
                d["prov:wasAttributedTo"] = enrichment[:data]["prov:wasAttributedTo"]
            end

            #pp d["prov:wasAttributedTo"] 

            d
            
        } ] }
    },

    rs_data_enrichment: {
        data: { "$._translation" => [ lambda { |d,o|  
            d["additionalType"] = "Translation_sv-Latn_to_en-Latn"
            
            {
                "prov:wasAttributedTo": [
                    {
                    "prov:wasAssociatedFor": [
                        {
                            "prov:used": [
                                {
                                "itemListElement": [
                                    "text",
                                    "description",
                                    "name"
                                ],
                                "@type": "itemListElement",
                                "name": "Used fields from the attributed entity",
                                "description": "list of fields from the record that are used in this enrichment process",
                                "@id": "used_fields_for_enrichment"
                                },
                                {
                                "name": "Google Neural Machine Translation model",
                                "@id": "google_nmt",
                                "url": "https://cloud.google.com/translate/docs/languages"
                                }
                            ],
                            "prov:generatedAtTime": "2024-02-28T18:46:05.935418",
                            "@type": [
                                "prov:Activity",
                                "action"
                            ],
                            "name": "Google Cloud Translation",
                            "@id": "google_cloud_translation_sv_to_en",
                            "prov:generated": d.map { |k,v| d[k].delete("@language_original"); {k => d[k]}  }
                        }
                    ],
                    "@type": [
                        "prov:Agent",
                        "agent"
                    ],
                    "prov:type": "prov:SoftwareAgent",
                    "name": "Google Cloud Translation",
                    "description": "Cloud Translation API uses Google's neural machine translation technology to let you dynamically translate text",
                    "@id": "google_cloud_translation",
                    "url": "https://cloud.google.com/translate"
                    }
                ]
            }
        }]}
    }
}
