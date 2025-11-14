#encoding: UTF-8 

# loop over the enrichment[:data] and check if it must be added to or replace the data in d
# based in ["prov:wasAttributedTo"][@id] and ["prov:wasAssociatedFor"]["@id"]
# The data model is 
# "prov:wasAttributedTo" : [ 
#   { 
#        @id: 
#        "prov:wasAssociatedFor" : [
#          {
#             @id:
#             "prov:generated":
#          }
#        ]
#    }
# ]
#
# What to do with the new values in "prov:generated"
# What if the @id is equal but the "prov:used" or the "prov:generatedAtTime" is different
# Add to existing of replace ?
#
# "prov:generated" will be merged to unique values.
# prov:wasAttributedTo.prov:wasAssociatedFor.@id must be equal

# ToDo enrichment[:data]["prov:wasAttributedTo"].nil?  ????
# ==> enrichment[:data].nil ==> Must "prov:wasAttributedTo" be deleted ?

def process_enrichment(enrichment, d)
  return if enrichment[:data].nil?

  enrichment_data = enrichment[:data]
  enrichment_data["prov:wasAttributedTo"] = ensure_array(enrichment_data["prov:wasAttributedTo"])

  if d["prov:wasAttributedTo"]
    d["prov:wasAttributedTo"] = ensure_array(d["prov:wasAttributedTo"])

    enrichment_data["prov:wasAttributedTo"].each do |enrich_attr|
      enrich_attr["prov:wasAssociatedFor"] = ensure_array(enrich_attr["prov:wasAssociatedFor"])
      enrichment_processed = false

      d["prov:wasAttributedTo"].map! do |existing_attr|
        if existing_attr["@id"] == enrich_attr["@id"]
          existing_attr["prov:wasAssociatedFor"] = ensure_array(existing_attr["prov:wasAssociatedFor"])

          enrich_attr["prov:wasAssociatedFor"].each do |enrich_assoc|
            enrich_assoc["prov:generated"] = ensure_array(enrich_assoc["prov:generated"])

            match_found = false
            existing_attr["prov:wasAssociatedFor"].map! do |existing_assoc|
              if existing_assoc["@id"] == enrich_assoc["@id"]
                enrich_assoc["prov:generated"] = merge_generated_data(enrich_assoc["prov:generated"], existing_assoc["prov:generated"])
                enrichment_processed = true
                match_found = true
              end
              existing_assoc
            end

            unless match_found
              existing_attr["prov:wasAssociatedFor"] << enrich_assoc
              enrichment_processed = true
            end
          end
        end
        existing_attr
      end

      unless enrichment_processed
        d["prov:wasAttributedTo"] << enrich_attr
      end
    end
  else
    d["prov:wasAttributedTo"] = enrichment_data["prov:wasAttributedTo"]
  end
  return d
end

