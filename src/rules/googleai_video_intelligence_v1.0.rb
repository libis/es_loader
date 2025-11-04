#encoding: UTF-8
require 'data_collector'
require "iso639"

include DataCollector::Core

GOOGLE_AI_VIDEO_INTELLIGENCE_API_v1_0 = {
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
                                }
                            ],
                            "prov:generatedAtTime": generatedAtTime,
                            "@type": [
                                "prov:Activity",
                                "action"
                            ],
                            "name": "Google Cloud Video Intelligence",
                            "@id": "#{o["prov:wasAssociatedFor_id"]}",
                            "prov:generated": {"annotation_results": o["enrichment"]['annotation_results'] }
                        }
                    ],
                    "@type": [
                        "prov:Agent",
                        "agent"
                    ],
                    "prov:type": "prov:SoftwareAgent",
                    "name": "Google Cloud Video Intelligence",
                    "description": "The Video Intelligence API enables the annotation of videos—whether stored locally, in Cloud Storage, or live-streamed—by applying contextual insights at multiple levels: across the entire video, by segment, by shot, and even frame-by-frame, using Google's advanced video analysis technology.",
                    "@id": "google_video_intelligence",
                    "url": "https://cloud.google.com/video-intelligence/docs"
                    }
                ]
            }
        }]}
    }
}
