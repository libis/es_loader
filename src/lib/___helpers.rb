class Array
  def uniqBeginString
    self.uniq!
    array = self
    if array.map { |string| string.class }.uniq === [String]
      array = self.select { |str|
        output = true
        self.each { |el|
          if str != el
            if el.start_with?(str)
              el.start_with?(str)
              output = false
            end
          end
        } 
        output
      }
    end
    array
  end
end

def deep_sort_hash(hash)
  sorted = hash.sort.to_h
  sorted.each do |key, value|
    if value.is_a?(Hash)
      sorted[key] = deep_sort_hash(value)
    elsif value.is_a?(Array)
      sorted[key] = value.map { |v| v.is_a?(Hash) ? deep_sort_hash(v) : v }
    end
  end
  sorted
end

def ensure_array(value)
  value.is_a?(Array) ? value : [value]
end

def merge_generated_data(existing, new_data)
  new_data = ensure_array(new_data)
  existing = ensure_array(existing)
  combined = new_data + existing
  combined.uniq { |h| deep_sort_hash(h).to_json }
end



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

