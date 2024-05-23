#encoding: UTF-8
require 'data_collector'
require "iso639"

include DataCollector::Core



GOOGLE_AI_TRANLATION_v1_0 = {
    version: "1.0",
    rs_data_enrichment: {
        data: { "$._translation" => [ lambda { |d,o|  

            # d["additionalType"] = o["additionalType"]
            {
                "prov:wasAttributedTo": [
                    {
                    "prov:wasAssociatedFor": [
                        {
                            "prov:used": [
                                {
                                "itemListElement": o["itemListElement"],
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
                            "@id": "#{o["prov:wasAssociatedFor_id"]}",
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
