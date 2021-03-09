module Ruby3
  module MapReduce
    DELIMITER = ' '
    def self.slice_text(text, count)
      array = text.split(DELIMITER)
      array.each_slice((array.size.to_f / count).round).map {|el| el.join(DELIMITER) }
    end
  end
end