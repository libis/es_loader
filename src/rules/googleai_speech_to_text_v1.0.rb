#encoding: UTF-8
require 'data_collector'
require "iso639"

include DataCollector::Core

GOOGLE_AI_SPEECH_TO_TEXT_API_v1_0 = {
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
                                "name": "Google Speech-to-Text Service",
                                "@id": "google_speech_to_text",
                                "url": "https://cloud.google.com/speech-to-text/docs"
                                }
                            ],
                            "prov:generatedAtTime": generatedAtTime,
                            "@type": [
                                "prov:Activity",
                                "action"
                            ],
                            "name": "Google Cloud Speech-to-Text",
                            "@id": "#{o["prov:wasAssociatedFor_id"]}",
                            "prov:generated": {"results": o["enrichment"]['results'] }
                        }
                    ],
                    "@type": [
                        "prov:Agent",
                        "agent"
                    ],
                    "prov:type": "prov:SoftwareAgent",
                    "name": "Google Cloud Speech-to-Text",
                    "description": "Speech-to-Text enables easy integration of Google speech recognition technologies into developer applications. Send audio and receive a text transcription from the Speech-to-Text API service.",
                    "@id": "google_speech_to_text",
                    "url": "https://cloud.google.com/speech-to-text/docs"
                    }
                ]
            }
        }]}
    }
}
