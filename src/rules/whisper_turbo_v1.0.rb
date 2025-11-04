#encoding: UTF-8
require 'data_collector'
require "iso639"

include DataCollector::Core

WHISPER_LARGE_TURBO_RULE_SET_v1_0 = {
    version: "1.0",
    rs_data_enrichment: {
        data: { "$" => [ lambda { |d,o|  
            generatedAtTime = o["enrichment"]["file_generatedAtTime"] || DateTime.now

            # d["additionalType"] = o["additionalType"]
            {
                "prov:wasAttributedTo": [
                    {
                    "prov:wasAssociatedFor": [
                        {
                            "prov:used": [
                                {
                                "itemListElement": o["enrichment_is_based_on"],
                                "@type": "itemListElement",
                                "name": "Used fields from the attributed entity",
                                "description": "list of fields from the record that are used in this enrichment process",
                                "@id": "used_fields_for_enrichment"
                                },
                                {
                                "name": "Whisper #{o["enrichment"]["model"]} Model",
                                "@id": "whisper_l#{o["enrichment"]["model"]}",
                                "url": "https://huggingface.co/openai/whisper-large-v3-turbo"
                                }
                            ],
                            "prov:generatedAtTime": generatedAtTime,
                            "@type": [
                                "prov:Activity",
                                "action"
                            ],
                            "name": "Whisper",
                            "@id": "#{o["prov:wasAssociatedFor_id"]}",
                            "prov:generated": {"result": o["enrichment"]['result'] }
                        }
                    ],
                    "@type": [
                        "prov:Agent",
                        "agent"
                    ],
                    "prov:type": "prov:SoftwareAgent",
                    "name": "Whisper Service",
                    "description": "Whisper is a state-of-the-art model for automatic speech recognition (ASR) and speech translation",
                    "@id": "whisper",
                    "url": "https://github.com/openai/whisper"
                    }
                ]
            }
        }]}
    }
}
