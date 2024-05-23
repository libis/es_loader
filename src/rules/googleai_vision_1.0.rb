#encoding: UTF-8
require 'data_collector'
require "iso639"

include DataCollector::Core

GOOGLE_AI_VISION_API_v1_0 = {
    version: "1.0",
    rs_data_enrichment: {
        data: { "@" => [ lambda { |d,o|  

            texts = d["texts"].map { |t| t["description"] }.sort.uniq.compact
            texts.map! { |t| 
                    {
                        "name": {
                            "@value": t,
                            "@language": "en-Latn"
                        }
                    } 
                }
            
            objects = d["objects"].map { |object| object["name"] }.sort.uniq.compact

            objects.map! { |object| 
                {
                    "name": {
                        "@value": object,
                        "@language": "en-Latn"
                    }
                } 
            }

            generatedAtTime = o["enrichment"]["file_generatedAtTime"] || DateTime.now

            rdata =  {
                    "prov:wasAttributedTo": {
                        "prov:wasAssociatedFor": [],
                        "@type": [
                            "prov:Agent",
                            "agent"
                        ],
                        "prov:type": "prov:SoftwareAgent",
                        "name": "Google Cloud Vision API",
                        "description": "Cloud Vision API interprets and analyzes visual data and derives meaningful information from digital images, videos, and other visual inputs",
                        "@id": "google_cloud_vision",
                        "url": "https://cloud.google.com/vision"
                    }
                }

            unless texts.empty?
                rdata[:"prov:wasAttributedTo"][:"prov:wasAssociatedFor"] << {
                            "prov:used": [
                                {
                                    "itemListElement": o["itemListElement"],
                                    "@type": "itemListElement",
                                    "name": "Used fields from the attributed entity",
                                    "description": "list of fields from the record that are used in this enrichment process",
                                    "@id": "used_fields_for_enrichment"
                                },
                                {
                                    "name": "Google Cloud Vision API Optical Character Recognition",
                                    "@id": "google_ocr",
                                    "url": "https://cloud.google.com/vision/docs/ocr"
                                },
                                {
                                    "name": "Google Cloud Vision API Detect handwriting",
                                    "@id": "google_handwriting",
                                    "url": "https://cloud.google.com/vision/docs/handwriting"
                                }
                            ],
                            "prov:generatedAtTime": generatedAtTime,
                            "@type": [
                                "prov:Activity",
                                "action"
                            ],
                            "name": "Google Cloud Vision API",
                            "@id": "#{o["prov:wasAssociatedFor_id"]}_ocr",
                            "prov:generated": texts
                        }
            end

            unless objects.empty?
                rdata[:"prov:wasAttributedTo"][:"prov:wasAssociatedFor"] << {
                            "prov:used": [
                                {
                                    itemListElement: o["itemListElement"],
                                    "@type": "itemListElement",
                                    "name": "Used fields from the attributed entity",
                                    "description": "list of fields from the record that are used in this enrichment process",
                                    "@id": "used_fields_for_enrichment"
                                },
                                {
                                    "name": "Google Cloud Vision API Object Localization",
                                    "@id": "google_objl",
                                    "url": "https://cloud.google.com/vision/docs/object-localizer"
                                }
                            ],
                            "prov:generatedAtTime": generatedAtTime,
                            "@type": [
                                "prov:Activity",
                                "action"
                            ],
                            "name": "Google Cloud Vision API",
                            "@id": "#{o["prov:wasAssociatedFor_id"]}_object_loc",
                            "prov:generated": objects
                        }
            end

            if rdata[:"prov:wasAttributedTo"][:"prov:wasAssociatedFor"].empty?
                rdata = nil
            end

            rdata

        }]}
    }
}
