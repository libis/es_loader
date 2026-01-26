
# frozen_string_literal: true
module JsonPathPruner
  module_function

  # Main API
  def prune!(object, patterns)
    segment_patterns, full_path_regexes = normalize_patterns(patterns)

    #pp "segment_patterns:"
    #pp segment_patterns
    #pp "full_path_regexes:"
    #pp full_path_regexes
    # 1) Segment-level patterns
    segment_patterns.each do |segments|
      delete_by_segments!(object, segments)
    end

    # 2) Full path regexes
    unless full_path_regexes.empty?
      delete_by_full_path_regexes!(object, full_path_regexes)
    end

    object
  end

  # -------------------------
  # Normalization
  # -------------------------
  def normalize_patterns(patterns)
    segment_patterns = []
    full_path_regexes = []

    patterns.each do |pat|

      if pat.match?(/^\/.*\/$/) # "Starts / and ends /"
        pat = Regexp.new(pat[1..-2])
      else
        if pat.match?(/^\/.*\/i+$/) # "Starts / and ends /i"
          pat = Regexp.new(pat[1..-3], Regexp::IGNORECASE)
        end
      end

      case pat
      when String
        if glob?(pat)
          full_path_regexes << glob_to_full_path_regex(pat)
        else
          segment_patterns << pat.split(".")
        end
      when Array
        segment_patterns << pat.map { |seg| glob?(seg) ? glob_to_segment_regex(seg) : seg }
      when Regexp
        full_path_regexes << pat
      else
        raise ArgumentError, "Unsupported pattern type: #{pat.inspect}"
      end
    end

    [segment_patterns, full_path_regexes]
  end
  private_class_method :normalize_patterns

  # Detect if value is a string and contains glob symbols
  def glob?(value)
    return false unless value.is_a?(String)

    # Treat as glob ONLY if string has glob syntax AND is NOT a dotted path
    return false if value.include?('.') && !value.match?(/[?*+]/)

    value.match?(/[*?]/)

  end

  private_class_method :glob?

  # -------------------------
  # Glob → Regex: full-path
  # -------------------------

  def glob_to_full_path_regex(glob)
    pattern = Regexp.escape(glob)

    # ** = any number of segments, including zero, with or without trailing dot
    # Correct: match a.b.c. OR a.b.c OR "" (zero segments)
    pattern.gsub!(/\\\*\\\*/, '(?:[^.]+(?:\.|$))*')

    # * = one segment (no dot)
    pattern.gsub!(/\\\*/, '[^.]+')

    # ? = any one char
    pattern.gsub!(/\\\?/, '.')

    # {a,b} = alternation
    pattern.gsub!(/\\\{([^}]+)\\\}/) do
      "(#{$1.split(',').map { |x| Regexp.escape(x) }.join('|')})"
    end

    /\A#{pattern}\z/
  end




  private_class_method :glob_to_full_path_regex

  # -------------------------
  # Glob → Regex: segment-level
  # ------------------------- 
  def glob_to_segment_regex(glob)
    # Escape everything except glob tokens we explicitly support
    pattern = glob.gsub(/([.+^$()|\\])/) { "\\#{$1}" } # escape regex metachars but keep glob chars

    # Handle negated and normal character classes: [abc], [!a]
    pattern.gsub!(/\[!(.*?)\]/, "[^\\1]")  # convert [!a] → [^a]
    pattern.gsub!(/\[(.*?)\]/, "[\\1]")    # keep simple [abc]

    # Glob wildcards inside a segment
    pattern.gsub!("**", ".*")  # allow ** but treat same as .*
    pattern.gsub!("*", ".*")   # * → any chars inside segment
    pattern.gsub!("?", ".")    # ? → single char

    /\A#{pattern}\z/
  end


  private_class_method :glob_to_segment_regex

  # -------------------------
  # Segment deletion
  # -------------------------
  def delete_by_segments!(node, segments)
    return if segments.empty?

    case node
    when Array
      node.each { |elem| delete_by_segments!(elem, segments) }

    when Hash
      head, *tail = segments

      if tail.empty?
        # Delete keys matching this segment
        case head
        when String
          node.delete(head)
        when Regexp
          node.keys.grep(head).each { |k| node.delete(k) }
        end
      else
        # Recurse into matching children       
        case head
        when String
          child = node[head]
          delete_by_segments!(child, tail) if child
        when Regexp
          node.each { |k, v| delete_by_segments!(v, tail) if k =~ head }
        end
      end
    end
  end
  private_class_method :delete_by_segments!

  # -------------------------
  # Full path deletion
  # -------------------------
  def delete_by_full_path_regexes!(node, regexes, parent = [])
    case node
    when Array
      node.each { |elem| delete_by_full_path_regexes!(elem, regexes, parent) }

    when Hash
      node.keys.each do |k|
        key_str = k.to_s
        full_path = (parent + [key_str]).join(".")

        if regexes.any? { |re| re.match?(full_path) }
          node.delete(k)
          next
        end

        delete_by_full_path_regexes!(node[k], regexes, parent + [key_str])
      end
    end
  end
  private_class_method :delete_by_full_path_regexes!
end
