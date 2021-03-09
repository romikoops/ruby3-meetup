module Ruby3
  module MapReduce
    CHAR_SET = ('a'..'z').to_a.freeze
    DEFAULT_MIN_CHARS = 1
    DEFAULT_MAX_CHARS = 25
    DUPLICATION_FACTOR = 5

    def self.generate_words(count, min_chars = DEFAULT_MIN_CHARS, max_chars = DEFAULT_MAX_CHARS, dup_factor = DUPLICATION_FACTOR)
      possible_lengths = (min_chars..max_chars).to_a
      res = []

      while res.size < count
        res << \
          if ([true] * dup_factor + [false]).sample
            next if res.empty?
            res.sample
          else
            length = possible_lengths.sample
            possible_lengths << length
            CHAR_SET.sample(length).join
          end
      end
      res
    end

    def self.generate_text(word_count)
      generate_words(word_count).shuffle.join(' ')
    end
  end
end